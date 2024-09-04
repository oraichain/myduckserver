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
	Insert(ctx *sql.Context, keyRows []sql.Row) error
	Delete(ctx *sql.Context, keyRows []sql.Row) error
	Update(ctx *sql.Context, keyRows []sql.Row, valueRows []sql.Row) error
	Close() error
}

type TableWriterProvider interface {
	// GetTableWriter returns a TableWriter for writing to the specified |table| in the specified |database|.
	GetTableWriter(
		ctx *sql.Context, engine *sqle.Engine,
		databaseName, tableName string,
		schema sql.Schema,
		columnCount, rowCount int,
		identifyColumns, dataColumns mysql.Bitmap,
		eventType EventType,
		foreignKeyChecksDisabled bool,
	) (TableWriter, error)
}
