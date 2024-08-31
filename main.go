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
	"flag"
	"fmt"

	"github.com/apecloud/myduckserver/meta"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/sirupsen/logrus"

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
	dbFile  = "mysql.db"
)

func checkDependencies() {
	_, err := translateWithSQLGlot("SELECT 1")
	if err != nil {
		panic("Python and SQLGlot are required to run the server. Please install them and try again.")
	}
}

func init() {
	flag.StringVar(&address, "address", address, "The address to bind to.")
	flag.IntVar(&port, "port", port, "The port to bind to.")
}

func main() {
	flag.Parse()
	checkDependencies()

	provider, err := meta.NewDBProvider(dbFile)
	if err != nil {
		logrus.Fatalln("Failed to open the database:", err)
	}
	defer provider.Close()

	engine := sqle.NewDefault(provider)

	builder := NewDuckBuilder(engine.Analyzer.ExecBuilder, provider.Storage(), provider.CatalogName())
	engine.Analyzer.ExecBuilder = builder

	if err := setPersister(provider, engine); err != nil {
		logrus.Fatalln("Failed to set the persister:", err)
	}

	registerReplicaController(provider, engine, provider.Storage())

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewServerWithHandler(config, engine, wrapSessionBuilder(provider), nil, wrapHandler(builder))
	if err != nil {
		panic(err)
	}
	if err = s.Start(); err != nil {
		panic(err)
	}
}
