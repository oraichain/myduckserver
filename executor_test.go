package main

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sqle "github.com/dolthub/go-mysql-server"
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

	provider := memory.NewDBProvider()
	builder := &DuckBuilder{
		provider: provider,
		base:     &mockNodeExecBuilder{},
	}
	builder.conns.Store(uint32(1), db)
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

func TestDuckBuilder_Insert(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := memory.NewDBProvider()
	builder := &DuckBuilder{
		provider: provider,
		base:     &mockNodeExecBuilder{},
	}
	builder.conns.Store(uint32(1), db)
	session := memory.NewSession(sql.NewBaseSessionWithClientServer("", sql.Client{}, 1), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session),
		sql.WithQuery("INSERT INTO test_table (id, name) VALUES (1, 'test')"))

	mock.ExpectExec("INSERT INTO test_table \\(id, name\\) VALUES \\(1, 'test'\\)").WillReturnResult(sqlmock.NewResult(1, 1))

	// Create a mock db and table
	mockDB := memory.NewDatabase("testdb")
	//mockTable := &mockTable{
	//	name: "test_table",
	//}
	node := plan.NewInsertInto(mockDB, nil, nil, true, nil, nil, true)
	iter, err := builder.Build(ctx, node, nil)
	assert.NoError(t, err)
	assert.NotNil(t, iter, "Iterator should not be nil")

	// Check the result of the insert operation
	row, err := iter.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, row, "Result row should not be nil")
	assert.Len(t, row, 1, "Result should have one column")
	okResult, ok := row[0].(types.OkResult)
	assert.True(t, ok, "Result should be of type types.OkResult")
	assert.Equal(t, uint64(1), okResult.RowsAffected, "Number of affected rows should be 1")

	// There should be no more rows
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err, "Second call to Next should return EOF")

	// Close the iterator
	err = iter.Close(ctx)
	assert.NoError(t, err, "Closing the iterator should not return an error")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDuckBuilder_CreateTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := memory.NewDBProvider()
	builder := &DuckBuilder{
		provider: provider,
		base:     &mockNodeExecBuilder{},
	}
	builder.conns.Store(uint32(1), db)
	session := memory.NewSession(sql.NewBaseSessionWithClientServer("", sql.Client{}, 1), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session),
		sql.WithQuery("CREATE TABLE test_table (id INT, name VARCHAR(255))"))

	// VARCHAR(255) is translated to TEXT(255) in DuckDB
	mock.ExpectExec("CREATE TABLE test_table \\(id INT, name TEXT\\(255\\)\\)").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("COMMENT ON TABLE testdb.test_table").WillReturnResult(sqlmock.NewResult(0, 0))

	// Create a mock db and table
	mockDB := memory.NewDatabase("testdb")
	// Define the schema for the new table
	schema := sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false},
		{Name: "name", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 255), Nullable: true},
	}
	// Create a TableSpec
	tableSpec := &plan.TableSpec{
		Schema: sql.NewPrimaryKeySchema(schema),
		// You might need to set other fields of TableSpec as needed
	}
	node := plan.NewCreateTable(mockDB, "test_table", true, false, tableSpec)
	iter, err := builder.Build(ctx, node, nil)
	assert.NoError(t, err)
	assert.NotNil(t, iter, "Iterator should not be nil")

	// Check the result of the insert operation
	row, err := iter.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, row, "Result row should not be nil")
	assert.Len(t, row, 1, "Result should have one column")
	okResult, ok := row[0].(types.OkResult)
	assert.True(t, ok, "Result should be of type types.OkResult")
	assert.Equal(t, uint64(0), okResult.RowsAffected, "Number of affected rows should be 0")

	// There should be no more rows
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err, "Second call to Next should return EOF")

	// Close the iterator
	err = iter.Close(ctx)
	assert.NoError(t, err, "Closing the iterator should not return an error")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDuckBuilder_DropTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := memory.NewDBProvider()
	builder := &DuckBuilder{
		provider: provider,
		base:     &mockNodeExecBuilder{},
	}
	builder.conns.Store(uint32(1), db)
	session := memory.NewSession(sql.NewBaseSessionWithClientServer("", sql.Client{}, 1), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session),
		sql.WithQuery("DROP TABLE test_table"))

	mock.ExpectExec("DROP TABLE test_table").WillReturnResult(sqlmock.NewResult(0, 0))

	// Create a mock db and table
	mockDB := memory.NewDatabase("testdb")
	mockTable := &mockTable{
		name: "test_table",
	}
	tableNode := plan.NewResolvedTable(mockTable, mockDB, nil)

	// Create a DropTable node
	node := plan.NewDropTable([]sql.Node{tableNode}, true)
	iter, err := builder.Build(ctx, node, nil)
	assert.NoError(t, err)
	assert.NotNil(t, iter, "Iterator should not be nil")

	// Check the result of the drop operation
	row, err := iter.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, row, "Result row should not be nil")
	assert.Len(t, row, 1, "Result should have one column")
	okResult, ok := row[0].(types.OkResult)
	assert.True(t, ok, "Result should be of type types.OkResult")
	assert.Equal(t, uint64(0), okResult.RowsAffected, "Number of affected rows should be 0")

	// There should be no more rows
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err, "Second call to Next should return EOF")

	// Close the iterator
	err = iter.Close(ctx)
	assert.NoError(t, err, "Closing the iterator should not return an error")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDuckBuilder_ShowTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := memory.NewDBProvider()
	builder := &DuckBuilder{
		provider: provider,
		base:     &mockNodeExecBuilder{},
	}
	builder.conns.Store(uint32(1), db)
	session := memory.NewSession(sql.NewBaseSessionWithClientServer("", sql.Client{}, 1), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session),
		sql.WithQuery("SHOW TABLES"))

	// Mock the result of SHOW TABLES
	rows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("table1").
		AddRow("table2").
		AddRow("table3")
	mock.ExpectQuery("SHOW TABLES").WillReturnRows(rows)

	// Create a mock database
	mockDB := memory.NewDatabase("testdb")

	// Create a ShowTables node
	node := plan.NewShowTables(mockDB, false, nil) // false for full_schema

	iter, err := builder.Build(ctx, node, nil)
	assert.NoError(t, err)
	assert.NotNil(t, iter, "Iterator should not be nil")

	// Check the results
	expectedTables := []string{"table1", "table2", "table3"}
	for i, expectedTable := range expectedTables {
		row, err := iter.Next(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, row, fmt.Sprintf("Result row %d should not be nil", i))
		assert.Len(t, row, 1, fmt.Sprintf("Result row %d should have one column", i))
		tableName, ok := row[0].(string)
		assert.True(t, ok, fmt.Sprintf("Result %d should be of type string", i))
		assert.Equal(t, expectedTable, tableName, fmt.Sprintf("Table name %d should match", i))
	}

	// There should be no more rows
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err, "Call to Next after last row should return EOF")

	// Close the iterator
	err = iter.Close(ctx)
	assert.NoError(t, err, "Closing the iterator should not return an error")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDuckBuilder_ShowCreateTable(t *testing.T) {
	provider := memory.NewDBProvider()

	session := memory.NewSession(sql.NewBaseSession(), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	engine := sqle.NewDefault(provider)
	_, _, err := engine.Query(ctx, "CREATE DATABASE test_schema")
	assert.NoError(t, err)

	ctx.SetCurrentDatabase("test_schema")

	// Mock the result of SHOW CREATE TABLE
	createTableStatement := `CREATE TABLE test_table (
    id INTEGER,
    name VARCHAR(255),
    PRIMARY KEY (id)
)`
	_, _, err = engine.Query(ctx, createTableStatement)
	assert.NoError(t, err)

	ctx = sql.NewContext(context.Background(), sql.WithSession(session),
		sql.WithQuery("SHOW CREATE TABLE test_table"))

	// Create a mock database and table
	mockDB := memory.NewDatabase("testdb")
	mockTable := &mockTable{
		name: "test_table",
	}
	tableNode := plan.NewResolvedTable(mockTable, mockDB, nil)

	// Create a ShowCreateTable node
	node := plan.NewShowCreateTable(tableNode, false)

	builder := &DuckBuilder{
		provider: provider,
		base:     engine.Analyzer.ExecBuilder,
	}
	iter, err := builder.Build(ctx, node, nil)
	assert.NoError(t, err)
	assert.NotNil(t, iter, "Iterator should not be nil")

	// Check the result
	row, err := iter.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, row, "Result row should not be nil")
	assert.Len(t, row, 2, "Result should have two columns")

	tableName, ok := row[0].(string)
	assert.True(t, ok, "First column should be of type string")
	assert.Equal(t, "test_table", tableName, "Table name should match")

	createStatement, ok := row[1].(string)
	assert.True(t, ok, "Second column should be of type string")
	assert.Equal(t, createTableStatement, createStatement, "Create table statement should match")

	// There should be no more rows
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err, "Second call to Next should return EOF")

	// Close the iterator
	err = iter.Close(ctx)
	assert.NoError(t, err, "Closing the iterator should not return an error")
}

func TestDuckBuilder_CreateDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := memory.NewDBProvider()
	builder := &DuckBuilder{
		provider: provider,
		base:     &mockNodeExecBuilder{},
	}
	builder.conns.Store(uint32(1), db)
	session := memory.NewSession(sql.NewBaseSessionWithClientServer("", sql.Client{}, 1), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session),
		sql.WithQuery("CREATE DATABASE test_db"))

	// translate DATABASE to SCHEMA in duckdb
	mock.ExpectExec(`CREATE SCHEMA (.*) "test_db"`).WillReturnResult(sqlmock.NewResult(0, 0))

	// Create a CreateDatabase node
	node := plan.NewCreateDatabase("test_db", true, sql.Collation_Default)

	iter, err := builder.Build(ctx, node, nil)
	assert.NoError(t, err)
	assert.NotNil(t, iter, "Iterator should not be nil")

	// Check the result of the create database operation
	row, err := iter.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, row, "Result row should not be nil")
	assert.Len(t, row, 1, "Result should have one column")
	okResult, ok := row[0].(types.OkResult)
	assert.True(t, ok, "Result should be of type types.OkResult")
	assert.Equal(t, uint64(0), okResult.RowsAffected, "Number of affected rows should be 0")

	// There should be no more rows
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err, "Second call to Next should return EOF")

	// Close the iterator
	err = iter.Close(ctx)
	assert.NoError(t, err, "Closing the iterator should not return an error")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDuckBuilder_DropDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := memory.NewDBProvider()
	builder := &DuckBuilder{
		provider: provider,
		base:     &mockNodeExecBuilder{},
	}
	builder.conns.Store(uint32(1), db)
	session := memory.NewSession(sql.NewBaseSessionWithClientServer("", sql.Client{}, 1), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session),
		sql.WithQuery("DROP DATABASE test_db"))

	// Mock the result of DROP DATABASE
	mock.ExpectExec(`DROP SCHEMA (.*) "test_db"`).WillReturnResult(sqlmock.NewResult(0, 0))

	// Create a DropDatabase node
	node := plan.NewDropDatabase("test_db", true)

	iter, err := builder.Build(ctx, node, nil)
	assert.NoError(t, err)
	assert.NotNil(t, iter, "Iterator should not be nil")

	// Check the result of the drop database operation
	row, err := iter.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, row, "Result row should not be nil")
	assert.Len(t, row, 1, "Result should have one column")
	okResult, ok := row[0].(types.OkResult)
	assert.True(t, ok, "Result should be of type types.OkResult")
	assert.Equal(t, uint64(0), okResult.RowsAffected, "Number of affected rows should be 0")

	// There should be no more rows
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err, "Second call to Next should return EOF")

	// Close the iterator
	err = iter.Close(ctx)
	assert.NoError(t, err, "Closing the iterator should not return an error")

	assert.NoError(t, mock.ExpectationsWereMet())
}
