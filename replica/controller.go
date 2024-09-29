package replica

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/binlog"
	"github.com/apecloud/myduckserver/binlogreplication"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/sirupsen/logrus"
)

type DeltaController struct {
	mutex  sync.Mutex
	tables map[tableIdentifier]*deltaAppender
	pool   *backend.ConnectionPool
}

func (c *DeltaController) GetDeltaAppender(
	databaseName, tableName string,
	schema sql.Schema,
) (binlogreplication.DeltaAppender, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.tables == nil {
		c.tables = make(map[tableIdentifier]*deltaAppender)
	}

	id := tableIdentifier{databaseName, tableName}
	appender, ok := c.tables[id]
	if ok {
		return appender, nil
	}
	appender, err := newDeltaAppender(schema)
	if err != nil {
		return nil, err
	}
	c.tables[id] = appender
	return appender, nil
}

// Flush writes the accumulated changes to the database.
func (c *DeltaController) Flush(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Due to DuckDB's lack of support for atomic MERGE INTO, we have to do the following two steps separately:
	//   1. Delete rows that are being updated.
	//   2. Insert new rows.
	// To guarantee the atomicity of the two steps, we have to wrap them in a transaction.
	// Again, due to DuckDB's limitations of indexes, specifically over-eagerly unique constraint checking,
	// if we do **DELETE then INSERT** in the same transaction, we would get
	//   a unique constraint violation error for INSERT,
	//   or data corruption for INSERT OR REPLACE|IGNORE INTO.
	//
	// This is a noteworthy pitfall and seems unlikely to be fixed in DuckDB in the near future,
	// but we have to live with it.
	//
	// On the other hand, fortunately enough, **INSERT OR REPLACE then DELETE** in the same transaction works fine.
	//
	// The ultimate solution is to wait for DuckDB to improve its index handling.
	// In the meantime, we could contribute a patch to DuckDB to support atomic MERGE INTO,
	// which is another way to avoid the issue elegantly.
	//
	// See:
	//  https://duckdb.org/docs/sql/indexes.html#limitations-of-art-indexes
	//  https://github.com/duckdb/duckdb/issues/14133

	tx, err := c.pool.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Share the buffer among all tables.
	buf := bytes.Buffer{}

	for table, appender := range c.tables {
		if err := c.updateTable(ctx, tx, table, appender, &buf); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (c *DeltaController) updateTable(
	ctx context.Context,
	tx *stdsql.Tx,
	table tableIdentifier,
	appender *deltaAppender,
	buf *bytes.Buffer,
) error {
	buf.Reset()

	schema := appender.BaseSchema() // schema of the base table
	record := appender.Build()
	defer record.Release()

	fmt.Println("record:", record)

	// TODO(fan): Switch to zero-copy Arrow ingestion once this PR is merged:
	//   https://github.com/marcboeker/go-duckdb/pull/283
	w := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))
	if err := w.Write(record); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	bytes := buf.Bytes()
	size := len(bytes)
	ptr := unsafe.Pointer(&bytes[0])
	ipcSQL := fmt.Sprintf(
		" FROM scan_arrow_ipc([{ptr: %d::ubigint, size: %d::ubigint}])",
		uintptr(ptr), size,
	)

	qualifiedTableName := catalog.ConnectIdentifiersANSI(table.dbName, table.tableName)

	pkColumns := make([]int, 0, 1) // Most tables have a single-column primary key
	for i, col := range schema {
		if col.PrimaryKey {
			pkColumns = append(pkColumns, i)
		}
	}
	pkList := catalog.QuoteIdentifierANSI(schema[pkColumns[0]].Name)
	for _, i := range pkColumns[1:] {
		pkList += ", " + catalog.QuoteIdentifierANSI(schema[i].Name)
	}

	// Use the following SQL to get the latest view of the rows being updated.
	//
	// SELECT r[0] as action, ...
	// FROM (
	//   SELECT
	//     pk1, pk2, ...,
	//     LAST(ROW(*COLUMNS(*)) ORDER BY txn_group, txn_seq, action) AS r
	//   FROM delta
	//   GROUP BY pk1, pk2, ...
	// )
	//
	// Note that an update generates two rows: one for DELETE and one for INSERT.
	// So the numeric value of DELETE action MUST be smaller than that of INSERT.
	augmentedSchema := appender.Schema()
	var builder strings.Builder
	builder.Grow(512)
	builder.WriteString("SELECT ")
	builder.WriteString("r[1] AS ")
	builder.WriteString(catalog.QuoteIdentifierANSI(augmentedSchema[0].Name))
	for i, col := range augmentedSchema[1:] {
		builder.WriteString(", r[")
		builder.WriteString(strconv.Itoa(i + 2))
		builder.WriteString("]")
		if types.IsTimestampType(col.Type) {
			builder.WriteString("::TIMESTAMP")
		}
		builder.WriteString(" AS ")
		builder.WriteString(catalog.QuoteIdentifierANSI(col.Name))
	}
	builder.WriteString(" FROM (SELECT ")
	builder.WriteString(pkList)
	builder.WriteString(", LAST(ROW(*COLUMNS(*)) ORDER BY txn_group, txn_seq, action) AS r")
	builder.WriteString(ipcSQL)
	builder.WriteString(" GROUP BY ")
	builder.WriteString(pkList)
	builder.WriteString(")")
	condenseDeltaSQL := builder.String()

	var (
		result       stdsql.Result
		rowsAffected int64
		err          error
	)

	// Create a temporary table to store the latest delta view.
	result, err = tx.ExecContext(ctx, "CREATE OR REPLACE TEMP TABLE delta AS "+condenseDeltaSQL)
	if err == nil {
		rowsAffected, err = result.RowsAffected()
	}
	if err != nil {
		return err
	}
	defer tx.ExecContext(ctx, "DROP TABLE IF EXISTS temp.main.delta")

	logrus.WithFields(logrus.Fields{
		"table": qualifiedTableName,
		"rows":  rowsAffected,
	}).Infoln("Delta created")

	// Insert or replace new rows (action = INSERT) into the base table.
	insertSQL := "INSERT OR REPLACE INTO " +
		qualifiedTableName +
		" SELECT * EXCLUDE (" + AugmentedColumnList + ") FROM temp.main.delta WHERE action = " +
		strconv.Itoa(int(binlog.InsertRowEvent))
	result, err = tx.ExecContext(ctx, insertSQL)
	if err == nil {
		rowsAffected, err = result.RowsAffected()
	}
	if err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"table": qualifiedTableName,
		"rows":  rowsAffected,
	}).Infoln("Inserted")

	// Delete rows that have been deleted.
	// The plan for `IN` is optimized to a SEMI JOIN,
	// which is more efficient than ordinary INNER JOIN.
	// DuckDB does not support multiple columns in `IN` clauses,
	// so we need to handle this case separately using the `row()` function.
	inTuple := pkList
	if len(pkColumns) > 1 {
		inTuple = "row(" + pkList + ")"
	}
	deleteSQL := "DELETE FROM " + qualifiedTableName +
		" WHERE " + inTuple + " IN (SELECT " + inTuple +
		"FROM temp.main.delta WHERE action = " + strconv.Itoa(int(binlog.DeleteRowEvent)) + ")"
	result, err = tx.ExecContext(ctx, deleteSQL)
	if err == nil {
		rowsAffected, err = result.RowsAffected()
	}
	if err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"table": qualifiedTableName,
		"rows":  rowsAffected,
	}).Infoln("Deleted")

	return nil
}
