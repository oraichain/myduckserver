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

	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

type Session struct {
	*memory.Session
	db *catalog.DatabaseProvider
}

// NewSessionBuilder returns a session builder for the given database provider.
func NewSessionBuilder(provider *catalog.DatabaseProvider) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	_, err := provider.Storage().Exec("CREATE TABLE IF NOT EXISTS main.persistent_variables (name TEXT PRIMARY KEY, value TEXT, type TEXT)")
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
		return Session{memSession, provider}, nil
	}
}

var _ sql.PersistableSession = (*Session)(nil)

// PersistGlobal implements sql.PersistableSession.
func (sess Session) PersistGlobal(sysVarName string, value interface{}) error {
	if _, _, ok := sql.SystemVariables.GetGlobal(sysVarName); !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	_, err := sess.db.Storage().Exec(
		"INSERT OR REPLACE INTO main.persistent_variables (name, value, vtype) VALUES (?, ?, ?)",
		sysVarName, value, fmt.Sprintf("%T", value),
	)
	return err
}

// RemovePersistedGlobal implements sql.PersistableSession.
func (sess Session) RemovePersistedGlobal(sysVarName string) error {
	_, err := sess.db.Storage().Exec(
		"DELETE FROM main.persistent_variables WHERE name = ?",
		sysVarName,
	)
	return err
}

// RemoveAllPersistedGlobals implements sql.PersistableSession.
func (sess Session) RemoveAllPersistedGlobals() error {
	_, err := sess.db.Storage().Exec("DELETE FROM main.persistent_variables")
	return err
}

// GetPersistedValue implements sql.PersistableSession.
func (sess Session) GetPersistedValue(k string) (interface{}, error) {
	var value, vtype string
	err := sess.db.Storage().QueryRow(
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
