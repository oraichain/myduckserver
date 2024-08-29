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
package main

import (
	"context"

	"github.com/apecloud/myduckserver/meta"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

// copy from memory/session.go
// NewSessionBuilder returns a session for the given in-memory database provider suitable to use in a test server
// This can't be defined as server.SessionBuilder because importing it would create a circular dependency,
// but it's the same signature.
func NewSessionBuilder(pro *meta.DbProvider) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
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
		return memory.NewSession(baseSession, pro), nil
	}
}

var globals = make(map[string]interface{})

func wrapSessionBuilder(provider *meta.DbProvider) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		session, err := NewSessionBuilder(provider)(ctx, conn, addr)
		if err != nil {
			return nil, err
		}
		session.(*memory.Session).SetGlobals(globals)
		return session, nil
	}
}
