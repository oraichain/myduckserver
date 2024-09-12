package adapter

import (
	"context"
	stdsql "database/sql"

	"github.com/dolthub/go-mysql-server/sql"
)

type ConnectionHolder interface {
	GetConn(ctx context.Context) (*stdsql.Conn, error)
	GetCatalogConn(ctx context.Context) (*stdsql.Conn, error)
}

func GetConn(ctx *sql.Context) (*stdsql.Conn, error) {
	return ctx.Session.(ConnectionHolder).GetConn(ctx)
}

func QueryContext(ctx *sql.Context, query string, args ...any) (*stdsql.Rows, error) {
	conn, err := GetConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.QueryContext(ctx, query, args...)
}

// QueryCatalogContext is a helper function to query the catalog, such as information_schema.
// Unlike QueryContext, this function does not require a schema name to be set on the connection,
// and the current schema of the connection does not matter.
func QueryCatalogContext(ctx *sql.Context, query string, args ...any) (*stdsql.Rows, error) {
	conn, err := ctx.Session.(ConnectionHolder).GetCatalogConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.QueryContext(ctx, query, args...)
}

func ExecContext(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	conn, err := GetConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.ExecContext(ctx, query, args...)
}

// ExecCatalogContext is a helper function to execute a catalog modification query, such as creating a database.
// Unlike ExecContext, this function does not require a schema name to be set on the connection,
// and the current schema of the connection does not matter.
func ExecCatalogContext(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	conn, err := ctx.Session.(ConnectionHolder).GetCatalogConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.ExecContext(ctx, query, args...)
}
