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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type DuckBuilder struct {
	provider sql.MutableDatabaseProvider
	base     sql.NodeExecBuilder
	conns    map[uint32]*stdsql.DB
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
	fmt.Println("Query:", ctx.Query())
	fmt.Printf("Node Type: %T\n", n)
	fmt.Println("Node:", n)

	// Handle special queries
	switch ctx.Query() {
	case "select @@version_comment limit 1":
		return b.base.Build(ctx, root, r)
	case "SELECT DATABASE()":
		return b.base.Build(ctx, root, r)
	}

	conn, ok := b.conns[ctx.ID()]
	if !ok {
		c, err := stdsql.Open("duckdb", dbFile)
		if err != nil {
			return nil, err
		}
		b.conns[ctx.ID()] = c
		conn = c
	}

	switch node := n.(type) {
	case *plan.ResolvedTable:
		return b.executeQuery(ctx, node, conn, true)
	case sql.Expressioner:
		return b.executeExpressioner(ctx, node, conn)
	case *plan.CreateDB:
		if err := b.executeBase(ctx, node, r); err != nil {
			return nil, err
		}
		ctx = ctx.WithQuery(strings.Replace(
			strings.ToLower(strings.TrimSpace(ctx.Query())),
			"create database",
			"create schema",
			1,
		))
		return b.executeDDL(ctx, node, conn, false)
	case *plan.DropDB:
		if err := b.executeBase(ctx, node, r); err != nil {
			return nil, err
		}
		ctx = ctx.WithQuery(strings.Replace(
			strings.ToLower(strings.TrimSpace(ctx.Query())),
			"drop database",
			"drop schema",
			1,
		))
		return b.executeDDL(ctx, node, conn, false)
	case *plan.DropTable:
		return b.executeDDL(ctx, node, conn, true)
	case *plan.RenameTable:
		return b.executeDDL(ctx, node, conn, true)
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
		if err := b.executeBase(ctx, node, nil); err != nil {
			return nil, err
		}
		return b.executeDDL(ctx, n, conn, true)
	case *plan.AddColumn:
		return b.executeDDL(ctx, n, conn, true)
	case *plan.RenameColumn:
		return b.executeDDL(ctx, n, conn, true)
	case *plan.DropColumn:
		return b.executeDDL(ctx, n, conn, true)
	case *plan.ModifyColumn:
		return b.executeDDL(ctx, n, conn, true)
	case *plan.InsertInto:
		return b.executeDML(ctx, n, conn)
	case *plan.Update:
		return b.executeDML(ctx, n, conn)
	case *plan.ShowTables:
		return b.executeQuery(ctx, node, conn, false)
	default:
		return b.executeQuery(ctx, node, conn, true)
	}
}

func (b *DuckBuilder) executeQuery(ctx *sql.Context, n sql.Node, conn *stdsql.DB, needTranslate bool) (sql.RowIter, error) {
	fmt.Println("Executing Query...")
	duckSQL := ctx.Query()
	var err error
	if needTranslate {
		// Translate the MySQL query to a DuckDB query
		duckSQL, err = translate(ctx.Query())
		if err != nil {
			return nil, err
		}
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
	fmt.Println("Executing DML...")
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

func (b *DuckBuilder) executeDDL(ctx *sql.Context, n sql.Node, conn *stdsql.DB, needTranslate bool) (sql.RowIter, error) {
	fmt.Println("Executing DDL...")
	duckSQL := ctx.Query()
	var err error
	if needTranslate {
		// Translate the MySQL query to a DuckDB query
		duckSQL, err = translate(ctx.Query())
		if err != nil {
			return nil, err
		}
	}

	// Execute the DuckDB query
	_, err = conn.ExecContext(ctx.Context, duckSQL)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(types.OkResult{})), nil
}
