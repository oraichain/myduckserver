package main

import (
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
)

func TestSQLRowIter(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Define test data
	columns := []string{"id", "name", "age"}
	rows := sqlmock.NewRows(columns).
		AddRow(1, "Alice", 30).
		AddRow(2, "Bob", 25).
		AddRow(3, "Charlie", 35)

	// Set up the mock expectation
	mock.ExpectQuery("SELECT (.+) FROM users").WillReturnRows(rows)

	// Execute a query using the mock database
	dbRows, err := db.Query("SELECT id, name, age FROM users")
	if err != nil {
		t.Fatalf("error executing query: %s", err)
	}
	// Create the SQLRowIter
	schema := sql.Schema{
		{Name: "id", Type: types.Int64},
		{Name: "name", Type: types.Text},
		{Name: "age", Type: types.Int32},
	}
	iter := &SQLRowIter{rows: dbRows, schema: schema}

	// Test Next() method
	ctx := sql.NewEmptyContext()

	// First row
	row, err := iter.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, sql.NewRow(int64(1), "Alice", int64(30)), row)

	// Second row
	row, err = iter.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, sql.NewRow(int64(2), "Bob", int64(25)), row)

	// Third row
	row, err = iter.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, sql.NewRow(int64(3), "Charlie", int64(35)), row)

	// Should return io.EOF after last row
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err)

	// Test Close() method
	err = iter.Close(ctx)
	assert.NoError(t, err)

	// Ensure all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}
