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
	schema sql.Schema, columnCount int,
	identifyColumns, dataColumns mysql.Bitmap,
	eventType binlogreplication.EventType,
	foreignKeyChecksDisabled bool,
) (binlogreplication.TableWriter, error) {
	// if eventType == binlogreplication.InsertEvent {
	// 	return twp.newTableAppender(ctx, databaseName, tableName, columnCount)
	// }
	return twp.newTableUpdater(ctx, databaseName, tableName, schema, columnCount, identifyColumns, dataColumns, eventType)
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
	schema sql.Schema, columnCount int,
	identifyColumns, dataColumns mysql.Bitmap,
	eventType binlogreplication.EventType,
) (*tableUpdater, error) {
	tx, err := twp.db.BeginTx(ctx.Context, nil)
	if err != nil {
		return nil, err
	}

	var sql string
	replace := false
	fullTableName := quoteIdentifier(databaseName) + "." + quoteIdentifier(tableName)
	keyCount, dataCount := identifyColumns.BitCount(), dataColumns.BitCount()
	switch eventType {
	case binlogreplication.DeleteEvent:
		sql = "DELETE FROM " + fullTableName + " WHERE "
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
	case binlogreplication.UpdateEvent:
		if keyCount < columnCount || dataCount < columnCount || isPkUpdate(schema, identifyColumns, dataColumns) {
			sql = "UPDATE " + fullTableName + " SET "
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
		} else {
			sql = "INSERT OR REPLACE INTO " + fullTableName + " VALUES ("
			for i := range columnCount {
				sql += "?"
				if i < columnCount-1 {
					sql += ", "
				}
			}
			sql += ")"
			replace = true
		}
	case binlogreplication.InsertEvent:
		sql = "INSERT INTO " + fullTableName + " VALUES ("
		for i := range columnCount {
			sql += "?"
			if i < columnCount-1 {
				sql += ", "
			}
		}
		sql += ")"
	}

	logrus.WithFields(logrus.Fields{
		"sql": sql,
	}).Infoln("Creating table updater...")

	stmt, err := tx.PrepareContext(ctx.Context, sql)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	return &tableUpdater{
		tx:      tx,
		stmt:    stmt,
		replace: replace,
	}, nil
}

type tableUpdater struct {
	tx      *stdsql.Tx
	stmt    *stdsql.Stmt
	replace bool
}

var _ binlogreplication.TableWriter = &tableUpdater{}

func (tu *tableUpdater) Insert(ctx *sql.Context, row sql.Row) error {
	_, err := tu.stmt.ExecContext(ctx.Context, row...)
	return err
}

func (tu *tableUpdater) Delete(ctx *sql.Context, keys sql.Row) error {
	_, err := tu.stmt.ExecContext(ctx.Context, keys...)
	return err
}

func (tu *tableUpdater) Update(ctx *sql.Context, keys sql.Row, values sql.Row) error {
	if tu.replace {
		return tu.Insert(ctx, values)
	}
	// UPDATE t SET col1 = ?, col2 = ? WHERE key1 = ? AND key2 = ?
	args := make([]interface{}, len(keys)+len(values))
	copy(args, values)
	copy(args[len(values):], keys)
	_, err := tu.stmt.ExecContext(ctx.Context, args...)
	return err
}

func (tu *tableUpdater) Close() error {
	defer tu.tx.Commit()
	return tu.stmt.Close()
}

func quoteIdentifier(identifier string) string {
	return `"` + identifier + `"`
}
