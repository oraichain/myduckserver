// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package backend

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"sync"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/transpiler"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/sirupsen/logrus"
)

type DuckBuilder struct {
	base        sql.NodeExecBuilder
	db          *stdsql.DB
	catalogName string
	conns       sync.Map // map[uint32]*stdsql.Conn, but sync.Map is concurrent-safe
}

var _ sql.NodeExecBuilder = (*DuckBuilder)(nil)

func NewDuckBuilder(base sql.NodeExecBuilder, db *stdsql.DB, catalogName string) *DuckBuilder {
	return &DuckBuilder{
		base:        base,
		db:          db,
		catalogName: catalogName,
	}
}

func (b *DuckBuilder) GetConn(ctx context.Context, id uint32, schemaName string) (*stdsql.Conn, error) {
	entry, ok := b.conns.Load(id)
	conn := (*stdsql.Conn)(nil)

	if !ok {
		c, err := b.db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		b.conns.Store(id, c)
		conn = c
	} else {
		conn = entry.(*stdsql.Conn)
	}

	if schemaName != "" {
		var currentSchema string
		if err := conn.QueryRowContext(ctx, "SELECT CURRENT_SCHEMA()").Scan(&currentSchema); err != nil {
			logrus.WithError(err).Error("Failed to get current schema")
			return nil, err
		} else if currentSchema != schemaName {
			if _, err := conn.ExecContext(ctx, "USE "+catalog.FullSchemaName(b.catalogName, schemaName)); err != nil {
				if catalog.IsDuckDBSetSchemaNotFoundError(err) {
					return nil, sql.ErrDatabaseNotFound.New(schemaName)
				}
				logrus.WithField("schema", schemaName).WithError(err).Error("Failed to switch schema")
				return nil, err
			}
		}
	}
	return conn, nil
}

func (b *DuckBuilder) Build(ctx *sql.Context, root sql.Node, r sql.Row) (sql.RowIter, error) {
	n := root
	qp, ok := n.(*plan.QueryProcess)
	if ok {
		n = qp.Child()
	}
	tc, ok := n.(*plan.TransactionCommittingNode)
	if ok {
		n = tc.Child()
	}
	rua, ok := n.(*plan.RowUpdateAccumulator)
	if ok {
		n = rua.Child()
	}
	ctx.GetLogger().WithFields(logrus.Fields{
		"Query":    ctx.Query(),
		"NodeType": fmt.Sprintf("%T", n),
	}).Infoln("Building node:", n)

	// TODO; find a better way to fallback to the base builder
	switch n.(type) {
	case *plan.CreateDB, *plan.DropDB, *plan.DropTable, *plan.RenameTable,
		*plan.CreateTable, *plan.AddColumn, *plan.RenameColumn, *plan.DropColumn, *plan.ModifyColumn,
		*plan.CreateIndex, *plan.DropIndex, *plan.AlterIndex, *plan.ShowIndexes,
		*plan.ShowTables, *plan.ShowCreateTable,
		*plan.ShowBinlogs, *plan.ShowBinlogStatus, *plan.ShowWarnings:
		return b.base.Build(ctx, root, r)
	}

	// Fallback to the base builder if the plan contains system/user variables or is not a pure data query.
	if containsVariable(n) || !IsPureDataQuery(n) {
		return b.base.Build(ctx, root, r)
	}

	schemaName := ctx.Session.GetCurrentDatabase()

	conn, err := b.GetConn(ctx.Context, ctx.ID(), schemaName)
	if err != nil {
		return nil, err
	}

	switch node := n.(type) {
	case *plan.Use:
		useStmt := "USE " + catalog.FullSchemaName(b.catalogName, node.Database().Name())
		if _, err := conn.ExecContext(ctx.Context, useStmt); err != nil {
			if catalog.IsDuckDBSetSchemaNotFoundError(err) {
				return nil, sql.ErrDatabaseNotFound.New(node.Database().Name())
			}
			return nil, err
		}
		return b.base.Build(ctx, root, r)
	case *plan.StartTransaction:
		if _, err := conn.ExecContext(ctx.Context, "BEGIN TRANSACTION"); err != nil {
			return nil, err
		}
		return b.base.Build(ctx, root, r)
	case *plan.Commit:
		if _, err := conn.ExecContext(ctx.Context, "COMMIT"); err != nil {
			return nil, err
		}
		return b.base.Build(ctx, root, r)
	case *plan.Set:
		return b.base.Build(ctx, root, r)
	case *plan.ShowVariables:
		return b.base.Build(ctx, root, r)
	// SubqueryAlias is for select * from view
	case *plan.ResolvedTable, *plan.SubqueryAlias, *plan.TableAlias:
		return b.executeQuery(ctx, node, conn)
	case sql.Expressioner:
		return b.executeExpressioner(ctx, node, conn)
	case *plan.DeleteFrom:
		return b.executeDML(ctx, n, conn)
	case *plan.Truncate:
		return b.executeDML(ctx, n, conn)
	default:
		return b.base.Build(ctx, n, r)
	}
}

func (b *DuckBuilder) executeExpressioner(ctx *sql.Context, n sql.Expressioner, conn *stdsql.Conn) (sql.RowIter, error) {
	node := n.(sql.Node)
	switch n := n.(type) {
	case *plan.InsertInto:
		return b.executeDML(ctx, n, conn)
	case *plan.Update:
		return b.executeDML(ctx, n, conn)
	default:
		return b.executeQuery(ctx, node, conn)
	}
}

func (b *DuckBuilder) executeQuery(ctx *sql.Context, n sql.Node, conn *stdsql.Conn) (sql.RowIter, error) {
	logrus.Infoln("Executing Query...")

	var (
		duckSQL string
		err     error
	)

	// Translate the MySQL query to a DuckDB query
	switch n.(type) {
	case *plan.ShowTables:
		duckSQL = ctx.Query()
	default:
		duckSQL, err = transpiler.Translate(n, ctx.Query())
	}
	if err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"Query":   ctx.Query(),
		"DuckSQL": duckSQL,
	}).Infoln("Executing Query...")

	// Execute the DuckDB query
	rows, err := conn.QueryContext(ctx.Context, duckSQL)
	if err != nil {
		return nil, err
	}

	return NewSQLRowIter(rows, n.Schema())
}

func (b *DuckBuilder) executeDML(ctx *sql.Context, n sql.Node, conn *stdsql.Conn) (sql.RowIter, error) {
	// Translate the MySQL query to a DuckDB query
	duckSQL, err := transpiler.Translate(n, ctx.Query())
	if err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"Query":   ctx.Query(),
		"DuckSQL": duckSQL,
	}).Infoln("Executing DML...")

	// Execute the DuckDB query
	result, err := conn.ExecContext(ctx.Context, duckSQL)
	if err != nil {
		return nil, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	insertId, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(types.OkResult{
		RowsAffected: uint64(affected),
		InsertID:     uint64(insertId),
	})), nil
}

// containsVariable inspects if the plan contains a system or user variable.
func containsVariable(n sql.Node) bool {
	found := false
	transform.InspectExpressions(n, func(e sql.Expression) bool {
		switch e.(type) {
		case *expression.SystemVar, *expression.UserVar:
			found = true
			return false
		}
		return true
	})
	return found
}

// IsPureDataQuery inspects if the plan is a pure data query,
// i.e., it operates on (>=1) data tables and does not touch any system tables.
// The following examples are NOT pure data queries:
// - `SELECT * FROM mysql.*`
// - `TRUNCATE mysql.user`
// - `SELECT DATABASE()`
func IsPureDataQuery(n sql.Node) bool {
	c := &tableAndFuncCollector{}
	transform.Walk(c, n)

	hasDataTable := false
	for _, tn := range c.tables {
		switch tn.Database().Name() {
		case "mysql", "information_schema", "performance_schema", "sys":
			return false
		}
		switch tn.UnderlyingTable().(type) {
		case *catalog.Table, *catalog.IndexedTable:
			hasDataTable = true
		}
	}
	if !hasDataTable {
		return false
	}

	for _, fe := range c.functions {
		if _, ok := fe.(*function.Database); ok {
			return false
		}
	}
	return true
}

type tableAndFuncCollector struct {
	functions []sql.FunctionExpression
	tables    []sql.TableNode
}

type exprVisitor tableAndFuncCollector

func (v *exprVisitor) Visit(expr sql.Expression) sql.Visitor {
	if expr == nil {
		return nil
	} else if fe, ok := expr.(sql.FunctionExpression); ok {
		v.functions = append(v.functions, fe)
	}

	// Visit subquery nodes to collect any nested table references
	if en, ok := expr.(sql.ExpressionWithNodes); ok {
		for _, child := range en.NodeChildren() {
			transform.Walk((*tableAndFuncCollector)(v), child)
		}
	}

	return v
}

func (c *tableAndFuncCollector) Visit(n sql.Node) transform.Visitor {
	if n == nil {
		return nil
	} else if tn, ok := n.(sql.TableNode); ok {
		c.tables = append(c.tables, tn)
	}

	// Visit expressions to find functions e.g. database() and walk subquery nodes to collect any nested table references
	if en, ok := n.(sql.Expressioner); ok {
		for _, e := range en.Expressions() {
			sql.Walk((*exprVisitor)(c), e)
		}
	}

	return c
}
