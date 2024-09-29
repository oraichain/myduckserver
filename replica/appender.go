package replica

import (
	"database/sql/driver"

	"github.com/apecloud/myduckserver/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/marcboeker/go-duckdb"
)

func (twp *tableWriterProvider) newTableAppender(
	ctx *sql.Context,
	databaseName, tableName string,
	columnCount int,
) (*tableAppender, error) {
	connector := twp.pool.Connector()
	conn, err := connector.Connect(ctx.Context)
	if err != nil {
		connector.Close()
		return nil, err
	}

	txn, err := conn.(driver.ConnBeginTx).BeginTx(ctx.Context, driver.TxOptions{})
	if err != nil {
		conn.Close()
		connector.Close()
		return nil, err
	}

	appender, err := duckdb.NewAppenderFromConn(conn, databaseName, tableName)
	if err != nil {
		txn.Rollback()
		conn.Close()
		connector.Close()
		return nil, err
	}

	return &tableAppender{
		connector: connector,
		conn:      conn,
		txn:       txn,
		appender:  appender,
		buffer:    make([]driver.Value, columnCount),
	}, nil
}

type tableAppender struct {
	connector *duckdb.Connector
	conn      driver.Conn
	txn       driver.Tx
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

func (ta *tableAppender) Commit() error {
	defer ta.connector.Close()
	defer ta.conn.Close()
	defer ta.txn.Commit()
	return ta.appender.Close()
}

func (ta *tableAppender) Rollback() error {
	defer ta.connector.Close()
	defer ta.conn.Close()
	defer ta.txn.Rollback()
	return ta.appender.Close()
}
