package binlogreplication

import "github.com/dolthub/go-mysql-server/sql"

type WriteSession interface {
	Close() error
}

type TableWriter interface {
	Insert(ctx *sql.Context, row sql.Row) error
	Delete(ctx *sql.Context, row sql.Row) error
	Update(ctx *sql.Context, row sql.Row, values sql.Row) error
}
