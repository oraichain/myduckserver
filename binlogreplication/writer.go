package binlogreplication

import (
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apecloud/myduckserver/binlog"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"vitess.io/vitess/go/mysql"
)

type TableWriter interface {
	Insert(ctx *sql.Context, keyRows []sql.Row) error
	Delete(ctx *sql.Context, keyRows []sql.Row) error
	Update(ctx *sql.Context, keyRows []sql.Row, valueRows []sql.Row) error
	Commit() error
	Rollback() error
}

type DeltaAppender interface {
	Field(i int) array.Builder
	Fields() []array.Builder
	Action() *array.Int8Builder
	TxnTag() *array.BinaryDictionaryBuilder
	TxnServer() *array.BinaryDictionaryBuilder
	TxnGroup() *array.BinaryDictionaryBuilder
	TxnSeqNumber() *array.Uint64Builder
}

type TableWriterProvider interface {
	// GetTableWriter returns a TableWriter for writing to the specified |table| in the specified |database|.
	GetTableWriter(
		ctx *sql.Context, engine *sqle.Engine,
		databaseName, tableName string,
		schema sql.PrimaryKeySchema,
		columnCount, rowCount int,
		identifyColumns, dataColumns mysql.Bitmap,
		eventType binlog.RowEventType,
		foreignKeyChecksDisabled bool,
	) (TableWriter, error)

	// GetDeltaAppender returns an ArrowAppender for appending updates to the specified |table| in the specified |database|.
	GetDeltaAppender(
		ctx *sql.Context, engine *sqle.Engine,
		databaseName, tableName string,
		schema sql.Schema,
	) (DeltaAppender, error)

	// FlushDelta writes the accumulated changes to the database.
	FlushDelta(ctx *sql.Context) error
}
