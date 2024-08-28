package main

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSchemas(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Create a mock engine
	provider := memory.NewDBProvider()
	engine := sqle.NewDefault(provider)
	session := memory.NewSession(sql.NewBaseSession(), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	// Mock schema query
	mock.ExpectQuery("SELECT DISTINCT schema_name FROM information_schema.schemata").
		WillReturnRows(sqlmock.NewRows([]string{"schema_name"}).
			AddRow("information_schema").
			AddRow("main").
			AddRow("pg_catalog").
			AddRow("test_schema"))

	// Call the function we're testing
	err = loadSchemas(ctx, db, engine)
	assert.NoError(t, err)

	// Verify that the schema was created
	schemas := engine.Analyzer.Catalog.AllDatabases(ctx)
	schemaNames := make([]string, len(schemas))
	for i, schema := range schemas {
		schemaNames[i] = schema.Name()
	}
	assert.Contains(t, schemaNames, "test_schema")
	assert.Contains(t, schemaNames, "information_schema")
	assert.NotContains(t, schemaNames, "main")
	assert.NotContains(t, schemaNames, "pg_catalog")

	// Ensure all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestLoadTables(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Create a mock engine
	provider := memory.NewDBProvider()
	engine := sqle.NewDefault(provider)
	session := memory.NewSession(sql.NewBaseSession(), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	_, _, _, err = engine.Query(ctx, "CREATE DATABASE test_schema")
	assert.NoError(t, err)

	// Mock table query
	ddl := "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50))"
	encodedDDL := base64.StdEncoding.EncodeToString([]byte(ddl))

	mock.ExpectQuery("SELECT DISTINCT schema_name, table_name, comment FROM duckdb_tables()").
		WillReturnRows(sqlmock.NewRows([]string{"schema_name", "table_name", "comment"}).
			AddRow("test_schema", "test_table", encodedDDL))

	// Call the function we're testing
	err = loadTables(ctx, db, engine)
	assert.NoError(t, err)

	// Verify that the tables was created
	ctx.SetCurrentDatabase("test_schema")
	schema, err := engine.Analyzer.Catalog.Database(ctx, "test_schema")
	assert.NoError(t, err)
	tables, err := schema.GetTableNames(ctx)
	assert.Contains(t, tables, "test_table")

	// Ensure all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}
