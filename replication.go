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
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"

	"github.com/apecloud/myduckserver/binlogreplication"
	"github.com/apecloud/myduckserver/meta"
)

// registerReplicaController registers the replica controller into the engine
// to handle the replication commands, such as START REPLICA, STOP REPLICA, etc.
func registerReplicaController(provider *meta.DbProvider, engine *sqle.Engine, db *stdsql.DB) {
	replica := binlogreplication.MyBinlogReplicaController
	replica.SetEngine(engine)

	session := memory.NewSession(sql.NewBaseSession(), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("mysql")
	replica.SetExecutionContext(ctx)

	replica.SetTableWriterProvider(&tableWriterProvider{db: db})

	engine.Analyzer.Catalog.BinlogReplicaController = binlogreplication.MyBinlogReplicaController

	// If we're unable to restart replication, log an error, but don't prevent the server from starting up
	if err := binlogreplication.MyBinlogReplicaController.AutoStart(ctx); err != nil {
		logrus.Errorf("unable to restart replication: %s", err.Error())
	}
}

type tableWriterProvider struct {
	db *stdsql.DB
}

var _ binlogreplication.TableWriterProvider = &tableWriterProvider{}

func (twp *tableWriterProvider) GetTableWriter(
	ctx *sql.Context, engine *sqle.Engine,
	databaseName, tableName string,
	schema sql.Schema,
	columnCount, rowCount int,
	identifyColumns, dataColumns mysql.Bitmap,
	eventType binlogreplication.EventType,
	foreignKeyChecksDisabled bool,
) (binlogreplication.TableWriter, error) {
	// if eventType == binlogreplication.InsertEvent {
	// 	return twp.newTableAppender(ctx, databaseName, tableName, columnCount)
	// }
	return twp.newTableUpdater(ctx, databaseName, tableName, schema, columnCount, rowCount, identifyColumns, dataColumns, eventType)
}

func (twp *tableWriterProvider) newTableAppender(
	ctx *sql.Context,
	databaseName, tableName string,
	columnCount int,
) (*tableAppender, error) {
	connector, err := duckdb.NewConnector(dbFilePath, nil)
	if err != nil {
		return nil, err
	}
	conn, err := connector.Connect(ctx.Context)
	if err != nil {
		connector.Close()
		return nil, err
	}

	appender, err := duckdb.NewAppenderFromConn(conn, databaseName, tableName)
	if err != nil {
		conn.Close()
		connector.Close()
		return nil, err
	}

	return &tableAppender{
		connector: connector,
		conn:      conn,
		appender:  appender,
		buffer:    make([]driver.Value, columnCount),
	}, nil
}

type tableAppender struct {
	connector *duckdb.Connector
	conn      driver.Conn
	appender  *duckdb.Appender
	buffer    []driver.Value
}

var _ binlogreplication.TableWriter = &tableAppender{}

func (ta *tableAppender) Insert(ctx *sql.Context, rows []sql.Row) error {
	for _, row := range rows {
		for i, v := range row {
			ta.buffer[i] = v
		}
	}
	return ta.appender.AppendRow(ta.buffer...)
}

func (ta *tableAppender) Delete(ctx *sql.Context, keyRows []sql.Row) error {
	panic("not implemented")
}

func (ta *tableAppender) Update(ctx *sql.Context, keyRows []sql.Row, valueRows []sql.Row) error {
	panic("not implemented")
}

func (ta *tableAppender) Close() error {
	defer ta.connector.Close()
	defer ta.conn.Close()
	return ta.appender.Close()
}

func isPkUpdate(schema sql.Schema, identifyColumns, dataColumns mysql.Bitmap) bool {
	for i, c := range schema {
		if c.PrimaryKey && identifyColumns.Bit(i) && dataColumns.Bit(i) {
			return true
		}
	}
	return false
}

func (twp *tableWriterProvider) newTableUpdater(
	ctx *sql.Context,
	databaseName, tableName string,
	schema sql.Schema,
	columnCount, rowCount int,
	identifyColumns, dataColumns mysql.Bitmap,
	eventType binlogreplication.EventType,
) (*tableUpdater, error) {
	tx, err := twp.db.BeginTx(ctx.Context, nil)
	if err != nil {
		return nil, err
	}

	var identifyIndex int
	var pkColumns []int
	var pkIndices []int
	for i, c := range schema {
		identify := identifyColumns.Count() > i && identifyColumns.Bit(i)
		if c.PrimaryKey && !identify {
			pkColumns = nil
			pkIndices = nil
			break
		} else if c.PrimaryKey && identify {
			pkColumns = append(pkColumns, i)
			pkIndices = append(pkIndices, identifyIndex)
		}
		if identify {
			identifyIndex++
		}
	}

	var (
		sql                 string
		paramCount          int
		pkUpdate            bool
		replace             = false
		reinsert            string
		fullTableName       = quoteIdentifier(databaseName) + "." + quoteIdentifier(tableName)
		keyCount, dataCount = identifyColumns.BitCount(), dataColumns.BitCount()
	)
	switch eventType {
	case binlogreplication.DeleteEvent:
		sql, paramCount = buildDeleteTemplate(fullTableName, columnCount, schema, pkColumns, identifyColumns)
	case binlogreplication.UpdateEvent:
		pkUpdate = isPkUpdate(schema, identifyColumns, dataColumns)
		if pkUpdate {
			// If the primary key is being updated, we need to use DELETE + INSERT.
			// For example, if the primary has executed `UPDATE t SET pk = pk + 1;`,
			// then both `UPDATE` and `REPLACE` will fail on the replica because the primary key is being updated:
			// - `UPDATE` will fail because of violation of the primary key constraint.
			// - `REPLACE` will fail because it will insert a new row but leave the old row unchanged.
			sql, paramCount = buildDeleteTemplate(fullTableName, columnCount, schema, pkColumns, identifyColumns)
			reinsert, _ = buildInsertTemplate(fullTableName, columnCount, false)
		} else if keyCount < columnCount || dataCount < columnCount {
			sql, paramCount = buildUpdateTemplate(fullTableName, columnCount, schema, pkColumns, identifyColumns, dataColumns)
		} else {
			sql, paramCount = buildInsertTemplate(fullTableName, columnCount, true)
			replace = true
		}
	case binlogreplication.InsertEvent:
		sql, paramCount = buildInsertTemplate(fullTableName, columnCount, false)
	}

	logrus.WithFields(logrus.Fields{
		"sql":       sql,
		"replace":   replace,
		"reinsert":  reinsert,
		"keyCount":  keyCount,
		"dataCount": dataCount,
		"pkUpdate":  pkUpdate,
	}).Infoln("Creating table updater...")

	stmt, err := tx.PrepareContext(ctx.Context, sql)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	return &tableUpdater{
		db:         twp.db,
		tx:         tx,
		stmt:       stmt,
		replace:    replace,
		reinsert:   reinsert,
		pkIndices:  pkIndices,
		paramCount: paramCount,
	}, nil
}

func buildInsertTemplate(tableName string, columnCount int, replace bool) (string, int) {
	var builder strings.Builder
	builder.Grow(32)
	builder.WriteString("INSERT")
	if replace {
		builder.WriteString(" OR REPLACE")
	}
	builder.WriteString(" INTO ")
	builder.WriteString(tableName)
	builder.WriteString(" VALUES (")
	for i := range columnCount {
		builder.WriteString("?")
		if i < columnCount-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(")")
	return builder.String(), columnCount
}

func buildDeleteTemplate(tableName string, columnCount int, schema sql.Schema, pkColumns []int, identifyColumns mysql.Bitmap) (string, int) {
	var builder strings.Builder
	builder.Grow(32)
	builder.WriteString("DELETE FROM ")
	builder.WriteString(tableName)
	builder.WriteString(" WHERE ")

	if len(pkColumns) > 0 {
		for i, c := range pkColumns {
			if i > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(quoteIdentifier(schema[c].Name))
			builder.WriteString(" = ?")
		}
		return builder.String(), len(pkColumns)
	}

	count := 0
	for i := range columnCount {
		if identifyColumns.Bit(i) {
			if count > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(quoteIdentifier(schema[i].Name))
			builder.WriteString(" = ?")
			count++
		}
	}
	return builder.String(), count
}

func buildUpdateTemplate(tableName string, columnCount int, schema sql.Schema, pkColumns []int, identifyColumns, dataColumns mysql.Bitmap) (string, int) {
	var builder strings.Builder
	builder.Grow(32)
	builder.WriteString("UPDATE ")
	builder.WriteString(tableName)
	builder.WriteString(" SET ")
	count := 0
	dataCount := dataColumns.BitCount()
	for i := range columnCount {
		if dataColumns.Bit(i) {
			if count > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(quoteIdentifier(schema[i].Name))
			builder.WriteString(" = ?")
			count++
		}
	}
	builder.WriteString(" WHERE ")

	if len(pkColumns) > 0 {
		for i, c := range pkColumns {
			if i > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(quoteIdentifier(schema[c].Name))
			builder.WriteString(" = ?")
		}
		return builder.String(), dataCount + len(pkColumns)
	}

	count = 0
	for i := range columnCount {
		if identifyColumns.Bit(i) {
			if count > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(quoteIdentifier(schema[i].Name))
			builder.WriteString(" = ?")
			count++
		}
	}
	return builder.String(), dataCount + identifyColumns.BitCount()
}

type tableUpdater struct {
	db         *stdsql.DB
	tx         *stdsql.Tx
	stmt       *stdsql.Stmt
	replace    bool
	reinsert   string
	paramCount int
	pkIndices  []int
}

var _ binlogreplication.TableWriter = &tableUpdater{}

func (tu *tableUpdater) Insert(ctx *sql.Context, rows []sql.Row) error {
	for _, row := range rows {
		if _, err := tu.stmt.ExecContext(ctx.Context, row...); err != nil {
			return err
		}
	}
	return nil
}

func (tu *tableUpdater) Delete(ctx *sql.Context, keyRows []sql.Row) error {
	buf := make(sql.Row, len(tu.pkIndices))
	for _, row := range keyRows {
		var keys sql.Row
		if len(tu.pkIndices) > 0 {
			for i, idx := range tu.pkIndices {
				buf[i] = row[idx]
			}
			keys = buf
		} else {
			keys = row
		}
		if _, err := tu.stmt.ExecContext(ctx.Context, keys...); err != nil {
			return err
		}
	}
	return nil
}

func (tu *tableUpdater) Update(ctx *sql.Context, keyRows []sql.Row, valueRows []sql.Row) error {
	if tu.replace {
		return tu.Insert(ctx, valueRows)
	}

	// https://duckdb.org/docs/sql/indexes#over-eager-unique-constraint-checking
	if tu.reinsert != "" {
		var err error
		// DELETE
		if err = tu.Delete(ctx, keyRows); err != nil {
			return err
		}
		if err = tu.stmt.Close(); err != nil {
			return err
		}
		if err = tu.tx.Commit(); err != nil {
			return err
		}

		tu.tx, err = tu.db.BeginTx(ctx.Context, nil)
		if err != nil {
			return err
		}
		tu.stmt, err = tu.tx.PrepareContext(ctx.Context, tu.reinsert)
		if err != nil {
			return err
		}
		// INSERT
		return tu.Insert(ctx, valueRows)
	}

	// UPDATE t SET col1 = ?, col2 = ? WHERE key1 = ? AND key2 = ?
	buf := make([]interface{}, tu.paramCount)
	for i, values := range valueRows {
		keys := keyRows[i]
		copy(buf, values)
		if len(tu.pkIndices) > 0 {
			for j, idx := range tu.pkIndices {
				buf[len(values)+j] = keys[idx]
			}
		} else {
			copy(buf[len(values):], keys)
		}
		args := buf[:len(values)+len(keys)]
		if _, err := tu.stmt.ExecContext(ctx.Context, args...); err != nil {
			return err
		}
	}
	return nil
}

func (tu *tableUpdater) Close() error {
	defer tu.tx.Commit()
	return tu.stmt.Close()
}

func quoteIdentifier(identifier string) string {
	return `"` + identifier + `"`
}
