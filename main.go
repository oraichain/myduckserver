// Copyright 2020-2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"

	stdsql "database/sql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"

	_ "github.com/marcboeker/go-duckdb"
)

// This is an example of how to implement a MySQL server.
// After running the example, you may connect to it using the following:
//
// > mysql --host=localhost --port=3306 --user=root
//
// The included MySQL client is used in this example, however any MySQL-compatible client will work.

var (
	address = "localhost"
	port    = 3306
	dbName  = "mysql"
	dbFile  = dbName + ".db"
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
		return b.executeQuery(ctx, node, conn)
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
		return b.executeDDL(ctx, node, conn)
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
		return b.executeDDL(ctx, node, conn)
	case *plan.DropTable:
		return b.executeDDL(ctx, node, conn)
	case *plan.RenameTable:
		return b.executeDDL(ctx, node, conn)
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
		return b.executeDDL(ctx, n, conn)
	case *plan.AddColumn:
		return b.executeDDL(ctx, n, conn)
	case *plan.RenameColumn:
		return b.executeDDL(ctx, n, conn)
	case *plan.DropColumn:
		return b.executeDDL(ctx, n, conn)
	case *plan.ModifyColumn:
		return b.executeDDL(ctx, n, conn)
	case *plan.InsertInto:
		return b.executeDML(ctx, n, conn)
	case *plan.Update:
		return b.executeDML(ctx, n, conn)
	default:
		return b.executeQuery(ctx, node, conn)
	}
}

func (b *DuckBuilder) executeQuery(ctx *sql.Context, n sql.Node, conn *stdsql.DB) (sql.RowIter, error) {
	fmt.Println("Executing Query...")
	// Translate the MySQL query to a DuckDB query
	duckSQL, err := translate(ctx.Query())
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

func (b *DuckBuilder) executeDDL(ctx *sql.Context, n sql.Node, conn *stdsql.DB) (sql.RowIter, error) {
	fmt.Println("Executing DDL...")
	// Translate the MySQL query to a DuckDB query
	duckSQL, err := translate(ctx.Query())
	if err != nil {
		return nil, err
	}

	// Execute the DuckDB query
	_, err = conn.ExecContext(ctx.Context, duckSQL)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(types.OkResult{})), nil
}

// translate converts a MySQL query to a DuckDB query using SQLGlot.
// For simplicity, we assume that Python and SQLGlot are installed on the system.
// Then we can call the following shell command to convert the query.
//
// python -c 'import sys; import sqlglot; sql = sys.stdin.read(); print(sqlglot.transpile(sql, read="mysql", write="duckdb")[0])
//
// In the future, we can deploy a SQLGlot server and use the API to convert the query.
func translate(mysqlQuery string) (string, error) {
	// Prepare the command to be executed
	cmd := exec.Command("python", "-c", `import sys; import sqlglot; sql = sys.stdin.read(); print(sqlglot.transpile(sql, read="mysql", write="duckdb")[0])`)

	// Set the input for the command
	cmd.Stdin = bytes.NewBufferString(mysqlQuery)

	// Capture the output of the command
	var out bytes.Buffer
	cmd.Stdout = &out

	// Execute the command
	if err := cmd.Run(); err != nil {
		return "", err
	}

	// Return the converted query
	return out.String(), nil
}

var _ sql.RowIter = (*SQLRowIter)(nil)

// SQLRowIter wraps a standard sql.Rows as a RowIter.
type SQLRowIter struct {
	rows   *stdsql.Rows
	schema sql.Schema
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
func (iter *SQLRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if !iter.rows.Next() {
		if err := iter.rows.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	columns, err := iter.rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := iter.rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// Prune the values to match the schema
	if len(values) > len(iter.schema) {
		values = values[:len(iter.schema)]
	} else if len(values) < len(iter.schema) {
		for i := len(values); i < len(iter.schema); i++ {
			values = append(values, nil)
		}
	}

	return sql.NewRow(values...), nil
}

// Close closes the underlying sql.Rows.
func (iter *SQLRowIter) Close(ctx *sql.Context) error {
	return iter.rows.Close()
}

func main() {
	pro := memory.NewDBProvider()
	engine := sqle.NewDefault(pro)

	builder := &DuckBuilder{provider: pro, base: engine.Analyzer.ExecBuilder, conns: make(map[uint32]*stdsql.DB)}
	engine.Analyzer.ExecBuilder = builder

	session := memory.NewSession(sql.NewBaseSession(), pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("mysql")

	// This variable may be found in the "users_example.go" file. Please refer to that file for a walkthrough on how to
	// set up the "mysql" database to allow user creation and user checking when establishing connections. This is set
	// to false for this example, but feel free to play around with it and see how it works.
	if enableUsers {
		if err := enableUserAccounts(ctx, engine); err != nil {
			panic(err)
		}
	}

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewServerWithHandler(config, engine, memory.NewSessionBuilder(pro), nil, wrapHandler(builder))
	if err != nil {
		panic(err)
	}
	if err = s.Start(); err != nil {
		panic(err)
	}
}

type MyHandler struct {
	*server.Handler
	builder *DuckBuilder
}

func (h *MyHandler) ConnectionClosed(c *mysql.Conn) {
	conn, ok := h.builder.conns[c.ConnectionID]
	if ok {
		if err := conn.Close(); err != nil {
			logrus.Warn("Failed to close connection:", err)
		}
	}
	h.Handler.ConnectionClosed(c)
}

func (h *MyHandler) ComInitDB(c *mysql.Conn, schemaName string) error {
	conn, ok := h.builder.conns[c.ConnectionID]
	if ok {
		if _, err := conn.Exec("USE " + dbName + "." + schemaName); err != nil {
			logrus.Warn("Failed to use "+schemaName+":", err)
		}
	}
	return h.Handler.ComInitDB(c, schemaName)
}

func wrapHandler(b *DuckBuilder) server.HandlerWrapper {
	return func(h mysql.Handler) (mysql.Handler, error) {
		handler, ok := h.(*server.Handler)
		if !ok {
			return nil, fmt.Errorf("expected *server.Handler, got %T", h)
		}

		return &MyHandler{
			Handler: handler,
			builder: b,
		}, nil
	}
}
