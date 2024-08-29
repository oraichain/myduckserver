package meta

import "gopkg.in/src-d/go-errors.v1"

var (
	ErrDuckDB = errors.NewKind("duckdb: %v")
)
