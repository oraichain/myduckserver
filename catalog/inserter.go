package catalog

import (
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/dolthub/go-mysql-server/sql"
)

type rowInserter struct {
	db     string
	table  string
	schema sql.Schema

	once     sync.Once
	conn     *stdsql.Conn
	tmpTable string
	stmt     *stdsql.Stmt
	err      error
}

var _ sql.RowInserter = &rowInserter{}

func (ri *rowInserter) init(ctx *sql.Context) {
	ri.tmpTable = fmt.Sprintf("%s_%s_%d", ri.db, ri.table, ctx.ID())
	ri.conn, ri.err = ctx.Session.(adapter.ConnectionHolder).GetConn(ctx)
	if ri.err != nil {
		return
	}
	ctx.GetLogger().WithField("db", ri.db).WithField("table", ri.table).Infoln("Creating temp table", ri.tmpTable)
	createTable := fmt.Sprintf(
		"CREATE TEMP TABLE IF NOT EXISTS %s AS FROM %s LIMIT 0",
		QuoteIdentifierANSI(ri.tmpTable),
		ConnectIdentifiersANSI(ri.db, ri.table),
	)
	if _, ri.err = ri.conn.ExecContext(ctx, createTable); ri.err != nil {
		return
	}

	// TODO(fan): Appender is faster, but it requires strict type alignment.
	var insert strings.Builder
	insert.WriteString("INSERT INTO ")
	insert.WriteString(QuoteIdentifierANSI(ri.tmpTable))
	insert.WriteString(" VALUES (")
	insert.WriteByte('?')
	for range ri.schema[1:] {
		insert.WriteString(", ?")
	}
	insert.WriteByte(')')
	ri.stmt, ri.err = ri.conn.PrepareContext(ctx, insert.String())
}

func (ri *rowInserter) StatementBegin(ctx *sql.Context) {
	ri.once.Do(func() {
		ri.init(ctx)
	})
}

func (ri *rowInserter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return ri.clear(ctx)
}

func (ri *rowInserter) StatementComplete(ctx *sql.Context) error {
	return ri.err
}

func (ri *rowInserter) Close(ctx *sql.Context) error {
	defer ri.clear(ctx)
	if ri.err == nil {
		sql := fmt.Sprintf("INSERT INTO %s SELECT * FROM temp.main.%s", ConnectIdentifiersANSI(ri.db, ri.table), QuoteIdentifierANSI(ri.tmpTable))
		_, ri.err = ri.conn.ExecContext(ctx, sql)
	}
	return ri.err
}

func (ri *rowInserter) Insert(ctx *sql.Context, row sql.Row) error {
	if ri.err != nil {
		return ri.err
	}
	if _, err := ri.stmt.ExecContext(ctx, row...); err != nil {
		ri.err = err
		return err
	}
	return nil
}

func (ri *rowInserter) clear(ctx *sql.Context) error {
	if ri.stmt != nil {
		ri.err = errors.Join(ri.err, ri.stmt.Close())
	}
	if ri.conn != nil {
		_, err := ri.conn.ExecContext(ctx, "DROP TABLE IF EXISTS temp.main."+QuoteIdentifierANSI(ri.tmpTable))
		ri.err = errors.Join(ri.err, err)
	}
	return ri.err
}
