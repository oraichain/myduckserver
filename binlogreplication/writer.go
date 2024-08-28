package binlogreplication

import (
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

type EventType int

const (
	InsertEvent EventType = iota
	DeleteEvent
	UpdateEvent
)

type TableWriter interface {
	Insert(ctx *sql.Context, row sql.Row) error
	Delete(ctx *sql.Context, keys sql.Row) error
	Update(ctx *sql.Context, keys sql.Row, values sql.Row) error
	Close() error
}

type TableWriterProvider interface {
	// GetTableWriter returns a TableWriter for writing to the specified |table| in the specified |database|.
	GetTableWriter(
		ctx *sql.Context, engine *sqle.Engine,
		databaseName, tableName string,
		schema sql.Schema, columnCount int,
		identifyColumns, dataColumns mysql.Bitmap,
		eventType EventType,
		foreignKeyChecksDisabled bool,
	) (TableWriter, error)
}
