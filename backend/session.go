// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package backend

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strconv"

	adapter "github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"
)

type Session struct {
	*memory.Session
	db   *catalog.DatabaseProvider
	pool *ConnectionPool
}

func NewSession(base *memory.Session, provider *catalog.DatabaseProvider, pool *ConnectionPool) *Session {
	return &Session{base, provider, pool}
}

// NewSessionBuilder returns a session builder for the given database provider.
func NewSessionBuilder(provider *catalog.DatabaseProvider, pool *ConnectionPool) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	_, err := pool.Exec("CREATE TABLE IF NOT EXISTS main.persistent_variables (name TEXT PRIMARY KEY, value TEXT, type TEXT)")
	if err != nil {
		panic(err)
	}

	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlConnectionUser, ok := conn.UserData.(sql.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}

		client := sql.Client{Address: host, User: user, Capabilities: conn.Capabilities}
		baseSession := sql.NewBaseSessionWithClientServer(addr, client, conn.ConnectionID)
		memSession := memory.NewSession(baseSession, provider)
		return Session{memSession, provider, pool}, nil
	}
}

var _ sql.TransactionSession = (*Session)(nil)
var _ sql.PersistableSession = (*Session)(nil)
var _ adapter.ConnectionHolder = (*Session)(nil)

type Transaction struct {
	memory.Transaction
	tx *stdsql.Tx
}

var _ sql.Transaction = (*Transaction)(nil)

// StartTransaction implements sql.TransactionSession.
func (sess Session) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	sess.GetLogger().Infoln("StartTransaction")
	base, err := sess.Session.StartTransaction(ctx, tCharacteristic)
	if err != nil {
		return nil, err
	}

	startUnderlyingTx := true
	if !ctx.GetIgnoreAutoCommit() {
		autocommit, err := plan.IsSessionAutocommit(ctx)
		if err != nil {
			return nil, err
		}
		if autocommit {
			// Don't start a DuckDB transcation if it is in autocommit mode
			startUnderlyingTx = false
		}
	}

	var tx *stdsql.Tx
	if startUnderlyingTx {
		sess.GetLogger().Infoln("StartDuckTransaction")
		tx, err = sess.GetTxn(ctx, &stdsql.TxOptions{ReadOnly: tCharacteristic == sql.ReadOnly})
		if err != nil {
			return nil, err
		}
	}
	return &Transaction{*base.(*memory.Transaction), tx}, nil
}

// CommitTransaction implements sql.TransactionSession.
func (sess Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	sess.GetLogger().Infoln("CommitTransaction")
	transaction := tx.(*Transaction)
	if transaction.tx != nil {
		sess.GetLogger().Infoln("CommitDuckTransaction")
		defer sess.CloseTxn()
		if err := transaction.tx.Commit(); err != nil {
			return err
		}
	}
	return sess.Session.CommitTransaction(ctx, &transaction.Transaction)
}

// Rollback implements sql.TransactionSession.
func (sess Session) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	sess.GetLogger().Infoln("Rollback")
	transaction := tx.(*Transaction)
	if transaction.tx != nil {
		sess.GetLogger().Infoln("RollbackDuckTransaction")
		defer sess.CloseTxn()
		if err := transaction.tx.Rollback(); err != nil {
			return err
		}
	}
	return sess.Session.Rollback(ctx, &transaction.Transaction)
}

// PersistGlobal implements sql.PersistableSession.
func (sess Session) PersistGlobal(sysVarName string, value interface{}) error {
	if _, _, ok := sql.SystemVariables.GetGlobal(sysVarName); !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	_, err := sess.ExecContext(
		context.Background(),
		"INSERT OR REPLACE INTO main.persistent_variables (name, value, vtype) VALUES (?, ?, ?)",
		sysVarName, value, fmt.Sprintf("%T", value),
	)
	return err
}

// RemovePersistedGlobal implements sql.PersistableSession.
func (sess Session) RemovePersistedGlobal(sysVarName string) error {
	_, err := sess.ExecContext(
		context.Background(),
		"DELETE FROM main.persistent_variables WHERE name = ?",
		sysVarName,
	)
	return err
}

// RemoveAllPersistedGlobals implements sql.PersistableSession.
func (sess Session) RemoveAllPersistedGlobals() error {
	_, err := sess.ExecContext(context.Background(), "DELETE FROM main.persistent_variables")
	return err
}

// GetPersistedValue implements sql.PersistableSession.
func (sess Session) GetPersistedValue(k string) (interface{}, error) {
	var value, vtype string
	err := sess.QueryRow(
		context.Background(),
		"SELECT value, vtype FROM main.persistent_variables WHERE name = ?", k,
	).Scan(&value, &vtype)
	switch {
	case err == stdsql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	default:
		switch vtype {
		case "string":
			return value, nil
		case "int":
			return strconv.Atoi(value)
		case "bool":
			return value == "true", nil
		default:
			return nil, fmt.Errorf("unknown variable type %s", vtype)
		}
	}
}

// GetConn implements adapter.ConnectionHolder.
func (sess Session) GetConn(ctx context.Context) (*stdsql.Conn, error) {
	return sess.pool.GetConnForSchema(ctx, sess.ID(), sess.GetCurrentDatabase())
}

// GetCatalogConn implements adapter.ConnectionHolder.
func (sess Session) GetCatalogConn(ctx context.Context) (*stdsql.Conn, error) {
	return sess.pool.GetConn(ctx, sess.ID())
}

func (sess Session) GetTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return sess.pool.GetTxn(ctx, sess.ID(), sess.GetCurrentDatabase(), options)
}

func (sess Session) CloseTxn() {
	sess.pool.CloseTxn(sess.ID())
}

func (sess Session) ExecContext(ctx context.Context, query string, args ...any) (stdsql.Result, error) {
	conn, err := sess.GetCatalogConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.ExecContext(ctx, query, args...)
}

func (sess Session) QueryRow(ctx context.Context, query string, args ...any) *stdsql.Row {
	conn, err := sess.GetCatalogConn(ctx)
	if err != nil {
		return nil
	}
	return conn.QueryRowContext(ctx, query, args...)
}
