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
	"database/sql/driver"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/marcboeker/go-duckdb"

	"github.com/apecloud/myduckserver/binlogreplication"
)

// registerReplicaController registers the replica controller into the engine
// to handle the replication commands, such as START REPLICA, STOP REPLICA, etc.
func registerReplicaController(provider *memory.DbProvider, engine *sqle.Engine) {
	replica := binlogreplication.MyBinlogReplicaController
	replica.SetEngine(engine)

	session := memory.NewSession(sql.NewBaseSession(), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("mysql")
	replica.SetExecutionContext(ctx)

	replica.SetTableWriterProvider(&tableWriterProvider{})

	engine.Analyzer.Catalog.BinlogReplicaController = binlogreplication.MyBinlogReplicaController
}

type tableWriterProvider struct{}

var _ binlogreplication.TableWriterProvider = &tableWriterProvider{}

func (twp *tableWriterProvider) GetTableWriter(
	ctx *sql.Context, engine *sqle.Engine,
	databaseName, tableName string,
	schema sql.Schema, columnCount int,
	identifyColumns, dataColumns mysql.Bitmap,
	eventType binlogreplication.EventType,
	foreignKeyChecksDisabled bool,
) (binlogreplication.TableWriter, error) {
	if eventType == binlogreplication.InsertEvent {
		return twp.newTableAppender(ctx, databaseName, tableName, columnCount)
	}
	return twp.newTableUpdater(ctx, databaseName, tableName, schema, columnCount, identifyColumns, dataColumns, eventType)
}

func (twp *tableWriterProvider) newTableAppender(
	ctx *sql.Context,
	databaseName, tableName string,
	columnCount int,
) (*tableAppender, error) {
	connector, err := duckdb.NewConnector(dbFile, nil)
	if err != nil {
		return nil, err
	}
	conn, err := connector.Connect(ctx.Context)
	if err != nil {
		connector.Close()
		return nil, err
	}

	tx, err := conn.(driver.ConnBeginTx).BeginTx(ctx.Context, driver.TxOptions{})
	if err != nil {
		conn.Close()
		connector.Close()
		return nil, err
	}

	appender, err := duckdb.NewAppenderFromConn(conn, databaseName, tableName)
	if err != nil {
		tx.Rollback()
		conn.Close()
		connector.Close()
		return nil, err
	}

	return &tableAppender{
		connector: connector,
		conn:      conn,
		tx:        tx,
		appender:  appender,
		buffer:    make([]driver.Value, columnCount),
	}, nil
}

type tableAppender struct {
	connector *duckdb.Connector
	conn      driver.Conn
	tx        driver.Tx
	appender  *duckdb.Appender
	buffer    []driver.Value
}

var _ binlogreplication.TableWriter = &tableAppender{}

func (ta *tableAppender) Insert(ctx *sql.Context, row sql.Row) error {
	for i, v := range row {
		ta.buffer[i] = v
	}
	return ta.appender.AppendRow(ta.buffer...)
}

func (ta *tableAppender) Delete(ctx *sql.Context, keys sql.Row) error {
	panic("not implemented")
}

func (ta *tableAppender) Update(ctx *sql.Context, keys sql.Row, values sql.Row) error {
	panic("not implemented")
}

func (ta *tableAppender) Close() error {
	defer ta.connector.Close()
	defer ta.conn.Close()
	defer ta.tx.Commit()
	return ta.appender.Close()
}

func (twp *tableWriterProvider) newTableUpdater(
	ctx *sql.Context,
	databaseName, tableName string,
	schema sql.Schema, columnCount int,
	identifyColumns, dataColumns mysql.Bitmap,
	eventType binlogreplication.EventType,
) (*tableUpdater, error) {
	db, err := stdsql.Open("duckdb", dbFile)
	if err != nil {
		return nil, err
	}

	tx, err := db.BeginTx(ctx.Context, nil)
	if err != nil {
		db.Close()
		return nil, err
	}

	var sql string
	fullTableName := quoteIdentifier(databaseName) + "." + quoteIdentifier(tableName)
	keyCount, dataCount := identifyColumns.BitCount(), dataColumns.BitCount()
	if eventType == binlogreplication.DeleteEvent {
		sql := "DELETE FROM " + fullTableName + " WHERE "
		count := 0
		for i := range columnCount {
			if !identifyColumns.Bit(i) {
				continue
			}
			sql += quoteIdentifier(schema[i].Name) + " = ?"
			count++
			if count < keyCount {
				sql += " AND "
			}
		}
	} else {
		sql := "UPDATE " + fullTableName + " SET "
		count := 0
		for i := range columnCount {
			if !dataColumns.Bit(i) {
				continue
			}
			sql += quoteIdentifier(schema[i].Name) + " = ?"
			count++
			if count < dataCount {
				sql += ", "
			}
		}
		sql += " WHERE "
		count = 0
		for i := range columnCount {
			if !identifyColumns.Bit(i) {
				continue
			}
			sql += quoteIdentifier(schema[i].Name) + " = ?"
			count++
			if count < keyCount {
				sql += " AND "
			}
		}
	}
	stmt, err := tx.PrepareContext(ctx.Context, sql)
	if err != nil {
		tx.Rollback()
		db.Close()
		return nil, err
	}

	return &tableUpdater{
		db:   db,
		tx:   tx,
		stmt: stmt,
	}, nil
}

type tableUpdater struct {
	db   *stdsql.DB
	tx   *stdsql.Tx
	stmt *stdsql.Stmt
}

var _ binlogreplication.TableWriter = &tableUpdater{}

func (tu *tableUpdater) Insert(ctx *sql.Context, row sql.Row) error {
	panic("not implemented")
}

func (tu *tableUpdater) Delete(ctx *sql.Context, keys sql.Row) error {
	_, err := tu.stmt.ExecContext(ctx.Context, keys...)
	return err
}

func (tu *tableUpdater) Update(ctx *sql.Context, keys sql.Row, values sql.Row) error {
	args := make([]interface{}, len(keys)+len(values))
	for i, v := range keys {
		args[i] = v
	}
	for i, v := range values {
		args[i+len(keys)] = v
	}
	_, err := tu.stmt.ExecContext(ctx.Context, args...)
	return err
}

func (tu *tableUpdater) Close() error {
	defer tu.db.Close()
	defer tu.tx.Commit()
	return tu.stmt.Close()
}

func quoteIdentifier(identifier string) string {
	return `"` + identifier + `"`
}
