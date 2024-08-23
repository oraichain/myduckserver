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
	stdsql "database/sql"
	"encoding/base64"
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
	conns    sync.Map // map[uint32]*stdsql.DB, but sync.Map is concurrent-safe
}

func (b *DuckBuilder) GetConn(id uint32) (*stdsql.DB, error) {
	entry, ok := b.conns.Load(id)
	if !ok {
		c, err := stdsql.Open("duckdb", dbFile)
		if err != nil {
			return nil, err
		}
		b.conns.Store(id, c)
		return c, nil
	}
	return entry.(*stdsql.DB), nil
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

	conn, err := b.GetConn(ctx.ID())
	if err != nil {
		return nil, err
	}

	switch node := n.(type) {
	case *plan.ShowCreateTable:
		return b.base.Build(ctx, n, r)
	case *plan.ResolvedTable:
		return b.executeQuery(ctx, node, conn)
	case sql.Expressioner:
		return b.executeExpressioner(ctx, node, conn)
	case *plan.CreateDB:
		return b.executeDDL(ctx, node, nil, conn)
	case *plan.DropDB:
		return b.executeDDL(ctx, node, nil, conn)
	case *plan.DropTable:
		return b.executeDDL(ctx, node, nil, conn)
	case *plan.RenameTable:
		return b.executeDDL(ctx, node, nil, conn)
	case *plan.DeleteFrom:
		return b.executeDML(ctx, n, conn)
	default:
		return b.base.Build(ctx, n, r)
	}
}

func (b *DuckBuilder) executeBase(ctx *sql.Context, n sql.Node, r sql.Row) error {
	if iter, err := b.base.Build(ctx, n, r); err != nil {
		return err
	} else {
		_, err = sql.RowIterToRows(ctx, iter)
		return err
	}
}

func (b *DuckBuilder) executeExpressioner(ctx *sql.Context, n sql.Expressioner, conn *stdsql.DB) (sql.RowIter, error) {
	node := n.(sql.Node)
	switch n := n.(type) {
	case *plan.CreateTable:
		return b.executeDDL(ctx, n, n, conn)
	case *plan.AddColumn:
		return b.executeDDL(ctx, n, n.Table, conn)
	case *plan.RenameColumn:
		return b.executeDDL(ctx, n, n.Table, conn)
	case *plan.DropColumn:
		return b.executeDDL(ctx, n, n.Table, conn)
	case *plan.ModifyColumn:
		return b.executeDDL(ctx, n, n.Table, conn)
	case *plan.InsertInto:
		return b.executeDML(ctx, n, conn)
	case *plan.Update:
		return b.executeDML(ctx, n, conn)
	case *plan.ShowTables:
		return b.executeQuery(ctx, node, conn)
	default:
		return b.executeQuery(ctx, node, conn)
	}
}

func (b *DuckBuilder) executeQuery(ctx *sql.Context, n sql.Node, conn *stdsql.DB) (sql.RowIter, error) {
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
		duckSQL, err = translate(ctx.Query())
	}
	if err != nil {
		return nil, err
	}

	// Execute the DuckDB query
	rows, err := conn.QueryContext(ctx.Context, duckSQL)
	if err != nil {
		return nil, err
	}

	// Create a new iterator
	iter := &SQLRowIter{rows: rows, schema: n.Schema()}

	return iter, nil
}

func (b *DuckBuilder) executeDML(ctx *sql.Context, n sql.Node, conn *stdsql.DB) (sql.RowIter, error) {
	logrus.Infoln("Executing DML...")

	// Translate the MySQL query to a DuckDB query
	duckSQL, err := translate(ctx.Query())
	if err != nil {
		return nil, err
	}

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

func (b *DuckBuilder) executeDDL(ctx *sql.Context, n sql.Node, table sql.Node, conn *stdsql.DB) (sql.RowIter, error) {
	logrus.Infoln("Executing DDL...")

	// Execute the DDL in the memory engine first
	if err := b.executeBase(ctx, n, nil); err != nil {
		return nil, err
	}

	var (
		duckSQL string
		err     error
	)
	switch n := n.(type) {
	case *plan.CreateDB:
		// Create a schema in DuckDB
		ifNotExists := ""
		if n.IfNotExists {
			ifNotExists = "IF NOT EXISTS"
		}
		duckSQL = fmt.Sprintf("CREATE SCHEMA %s %s", ifNotExists, n.DbName)
	case *plan.DropDB:
		// Drop a schema in DuckDB
		ifExists := ""
		if n.IfExists {
			ifExists = "IF EXISTS"
		}
		duckSQL = fmt.Sprintf("DROP SCHEMA %s %s", ifExists, n.DbName)
	default:
		// Translate the MySQL query to a DuckDB query
		duckSQL, err = translate(ctx.Query())
	}
	if err != nil {
		return nil, err
	}

	// Execute the DuckDB query
	_, err = conn.ExecContext(ctx.Context, duckSQL)
	if err != nil {
		return nil, err
	}

	// Save the table DDL to the DuckDB database
	if table != nil {
		err = b.SaveTableDDL(ctx, n, conn)
		if err != nil {
			return nil, err
		}
	}

	return sql.RowsToRowIter(sql.NewRow(types.OkResult{})), nil
}

func (b *DuckBuilder) SaveTableDDL(ctx *sql.Context, table sql.Node, conn *stdsql.DB) error {
	var ddl string
	switch table := table.(type) {
	case *plan.CreateTable:
		ddl = ctx.Query()
	default:
		showCtx := ctx.WithQuery("SHOW CREATE TABLE " + table.(sql.Nameable).Name())
		showNode := plan.NewShowCreateTable(table, false)
		iter, err := b.base.Build(showCtx, showNode, nil)
		if err != nil {
			return err
		}
		rows, err := sql.RowIterToRows(ctx, iter)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return fmt.Errorf("no rows returned from SHOW CREATE TABLE")
		}
		ddl = rows[0][1].(string)
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(ddl))

	name := table.(sql.Nameable).Name()
	if db, ok := table.(sql.Databaser); ok {
		db := db.Database().Name()
		name = fmt.Sprintf("%s.%s", db, name)
	}

	_, err := conn.ExecContext(ctx.Context, fmt.Sprintf("COMMENT ON TABLE %s IS '%s'", name, encoded))
	return err
}
