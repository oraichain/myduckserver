// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"

	stdsql "database/sql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"

	_ "github.com/marcboeker/go-duckdb"
)

// This is an example of how to implement a MySQL server.
// After running the example, you may connect to it using the following:
//
// > mysql --host=localhost --port=3306 --user=root
//
// The included MySQL client is used in this example, however any MySQL-compatible client will work.

var (
	address = "localhost"
	port    = 3306
	dbName  = "mysql"
	dbFile  = dbName + ".db"
)

func checkDependencies() {
	_, err := translate("SELECT 1")
	if err != nil {
		panic("Python and SQLGlot are required to run the server. Please install them and try again.")
	}
}

func main() {
	checkDependencies()

	pro := memory.NewDBProvider()
	engine := sqle.NewDefault(pro)

	builder := &DuckBuilder{provider: pro, base: engine.Analyzer.ExecBuilder, conns: make(map[uint32]*stdsql.DB)}
	engine.Analyzer.ExecBuilder = builder

	session := memory.NewSession(sql.NewBaseSession(), pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("mysql")

	// This variable may be found in the "users_example.go" file. Please refer to that file for a walkthrough on how to
	// set up the "mysql" database to allow user creation and user checking when establishing connections. This is set
	// to false for this example, but feel free to play around with it and see how it works.
	if enableUsers {
		if err := enableUserAccounts(ctx, engine); err != nil {
			panic(err)
		}
	}

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewServerWithHandler(config, engine, memory.NewSessionBuilder(pro), nil, wrapHandler(builder))
	if err != nil {
		panic(err)
	}
	if err = s.Start(); err != nil {
		panic(err)
	}
}
