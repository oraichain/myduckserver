package pgserver

import (
	"bufio"

	"github.com/dolthub/go-mysql-server/sql"
)

// DataLoader allows callers to insert rows from multiple chunks into a table. Rows encoded in each chunk will not
// necessarily end cleanly on a chunk boundary, so DataLoader implementations must handle recognizing partial, or
// incomplete records, and saving that partial record until the next call to LoadChunk, so that it may be prefixed
// with the incomplete record.
type DataLoader interface {
	// LoadChunk reads the records from |data| and inserts them into the previously configured table. Data records
	// are not guaranteed to stard and end cleanly on chunk boundaries, so implementations must recognize incomplete
	// records and save them to prepend on the next processed chunk.
	LoadChunk(ctx *sql.Context, data *bufio.Reader) error

	// Abort aborts the current load operation and releases all used resources.
	Abort(ctx *sql.Context) error

	// Finish finalizes the current load operation and commits the inserted rows so that the data becomes visibile
	// to clients. Implementations should check that the last call to LoadChunk did not end with an incomplete
	// record and return an error to the caller if so. The returned LoadDataResults describe the load operation,
	// including how many rows were inserted.
	Finish(ctx *sql.Context) (*LoadDataResults, error)
}

// LoadDataResults contains the results of a load data operation, including the number of rows loaded.
type LoadDataResults struct {
	// RowsLoaded contains the total number of rows inserted during a load data operation.
	RowsLoaded int32
}
