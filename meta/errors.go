package meta

import (
	"strings"

	"gopkg.in/src-d/go-errors.v1"
)

var (
	ErrDuckDB = errors.NewKind("duckdb: %v")
)

func IsDuckDBCatalogError(err error) bool {
	return strings.Contains(err.Error(), "Catalog Error")
}

func IsDuckDBTableAlreadyExistsError(err error) bool {
	return IsDuckDBCatalogError(err) && strings.Contains(err.Error(), "Table with name") && strings.Contains(err.Error(), "already exists")
}

func IsDuckDBTableNotFoundError(err error) bool {
	return IsDuckDBCatalogError(err) && strings.Contains(err.Error(), "Table with name") && strings.Contains(err.Error(), "does not exist")
}

func IsDuckDBViewNotFoundError(err error) bool {
	return IsDuckDBCatalogError(err) && strings.Contains(err.Error(), "View with name") && strings.Contains(err.Error(), "does not exist")
}

// ERROR 1105 (HY000): unknown error: Catalog Error: SET schema: No catalog + schema named "mysql.db0" found.
func IsDuckDBSetSchemaNotFoundError(err error) bool {
	return IsDuckDBCatalogError(err) && strings.Contains(err.Error(), "SET schema: No catalog + schema named")
}

// ERROR 1105 (HY000): duckdb: Catalog Error: Index with name "x_idx" already exists!
func IsDuckDBIndexAlreadyExistsError(err error) bool {
	return IsDuckDBCatalogError(err) && strings.Contains(err.Error(), "Index with name") && strings.Contains(err.Error(), "already exists")
}
