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
package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/sirupsen/logrus"
)

type DuckBuilder struct {
	provider sql.MutableDatabaseProvider
	base     sql.NodeExecBuilder
	db       *stdsql.DB
	conns    sync.Map // map[uint32]*stdsql.Conn, but sync.Map is concurrent-safe
}

func (b *DuckBuilder) GetConn(ctx context.Context, id uint32, schemaName string) (*stdsql.Conn, error) {
	entry, ok := b.conns.Load(id)
	if !ok {
		c, err := b.db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		b.conns.Store(id, c)
		return c, nil
	}
	conn := entry.(*stdsql.Conn)
	if schemaName != "" {
		var currentSchema string
		if err := conn.QueryRowContext(ctx, "SELECT CURRENT_SCHEMA()").Scan(&currentSchema); err != nil {
			logrus.WithError(err).Error("Failed to get current schema")
			return nil, err
		} else if currentSchema != schemaName {
			if _, err := conn.ExecContext(ctx, "USE "+dbName+"."+schemaName); err != nil {
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

	// Handle special queries
	switch ctx.Query() {
	case "select @@version_comment limit 1":
		return b.base.Build(ctx, root, r)
	case "SELECT DATABASE()":
		return b.base.Build(ctx, root, r)
	}
	// TODO; find a better way to fallback to the base builder
	switch n.(type) {
	case *plan.CreateDB, *plan.DropDB, *plan.DropTable, *plan.RenameTable,
		*plan.CreateTable, *plan.AddColumn, *plan.RenameColumn, *plan.DropColumn, *plan.ModifyColumn,
		*plan.ShowTables, *plan.ShowCreateTable:
		return b.base.Build(ctx, root, r)
	}

	schemaName := ctx.Session.GetCurrentDatabase()

	conn, err := b.GetConn(ctx.Context, ctx.ID(), schemaName)
	if err != nil {
		return nil, err
	}

	switch node := n.(type) {
	case *plan.Use:
		useStmt := "USE " + fullSchemaName(dbName, node.Database().Name())
		if _, err := conn.ExecContext(ctx.Context, useStmt); err != nil {
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
	case *plan.ResolvedTable, *plan.SubqueryAlias:
		return b.executeQuery(ctx, node, conn)
	case sql.Expressioner:
		return b.executeExpressioner(ctx, node, conn)
	case *plan.DeleteFrom:
		return b.executeDML(ctx, n, conn)
	case *plan.Truncate:
		if node.DatabaseName() == "mysql" {
			return sql.RowsToRowIter(sql.NewRow(types.OkResult{})), nil
		}
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
		duckSQL, err = translate(n, ctx.Query())
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

	// Create a new iterator
	iter := &SQLRowIter{rows: rows, schema: n.Schema()}

	return iter, nil
}

func (b *DuckBuilder) executeDML(ctx *sql.Context, n sql.Node, conn *stdsql.Conn) (sql.RowIter, error) {
	// Translate the MySQL query to a DuckDB query
	duckSQL, err := translate(n, ctx.Query())
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

func fullSchemaName(db, schema string) string {
	if db == "" {
		return schema
	}
	if schema == "" {
		return db
	}
	return db + "." + schema
}
