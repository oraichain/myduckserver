package main

import (
	"context"
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock implementation of sql.NodeExecBuilder
type mockNodeExecBuilder struct{}

func (m *mockNodeExecBuilder) Build(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// mockTable implements sql.Table
type mockTable struct {
	name string
}

func (m *mockTable) Name() string {
	return m.name
}

func (m *mockTable) String() string {
	return m.name
}

func (m *mockTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "id", Type: types.Int64},
		{Name: "name", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 255)},
	}
}

func (m *mockTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(), nil
}

func (m *mockTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

func (m *mockTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func TestDuckBuilder_Select(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	provider := memory.NewDBProvider()
	builder := NewDuckBuilder(&mockNodeExecBuilder{}, db, "")
	builder.conns.Store(uint32(1), conn)
	session := memory.NewSession(sql.NewBaseSessionWithClientServer("", sql.Client{}, 1), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session), sql.WithQuery("SELECT * FROM test_table"))

	mock.ExpectQuery("SELECT \\* FROM test_table").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test"))

	// Create a mock db and table
	mockDB := memory.NewDatabase("testdb")
	mockTable := &mockTable{
		name: "test_table",
	}
	node := plan.NewResolvedTable(mockTable, mockDB, "")
	iter, err := builder.Build(ctx, node, nil)
	assert.NoError(t, err)
	assert.NotNil(t, iter, "Iterator should not be nil")

	// Check the content of the row
	row, err := iter.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, row, "First row should not be nil")
	assert.Equal(t, 2, len(row), "Row should have 2 columns")
	assert.Equal(t, int64(1), row[0], "First column should be 1")
	assert.Equal(t, "test", row[1], "Second column should be 'test'")

	// Check for end of iterator
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err, "Second call to Next should return EOF")

	// Close the iterator
	err = iter.Close(ctx)
	assert.NoError(t, err, "Closing the iterator should not return an error")

	assert.NoError(t, mock.ExpectationsWereMet())
}
