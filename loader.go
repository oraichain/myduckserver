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
	stdsql "database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
)

func Load(provider *memory.DbProvider, engine *sqle.Engine) error {
	if _, err := os.Stat(dbFile); errors.Is(err, os.ErrNotExist) {
		// No database file, nothing to load
		return nil
	}

	session := memory.NewSession(sql.NewBaseSession(), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	conn, err := stdsql.Open("duckdb", dbFile+"?access_mode=read_only")
	if err != nil {
		return err
	}
	defer conn.Close()

	if err = loadSchemas(ctx, conn, engine); err != nil {
		return err
	}
	if err = loadTables(ctx, conn, engine); err != nil {
		return err
	}
	return nil
}

func loadSchemas(ctx *sql.Context, conn *stdsql.DB, engine *sqle.Engine) error {
	logrus.Infoln("Loading schemas...")

	rows, err := conn.Query(fmt.Sprintf("SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = '%s'", dbName))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return err
		}

		switch schemaName {
		case "information_schema", "main", "pg_catalog":
			continue
		}

		_, iter, err := engine.Query(ctx, "CREATE DATABASE "+schemaName)
		if err != nil {
			return err
		}
		_, err = sql.RowIterToRows(ctx, iter)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadTables(ctx *sql.Context, conn *stdsql.DB, engine *sqle.Engine) error {
	logrus.Infoln("Loading tables...")

	rows, err := conn.Query("SELECT DISTINCT schema_name, table_name, comment FROM duckdb_tables()")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName, comment string
		if err := rows.Scan(&schemaName, &tableName, &comment); err != nil {
			return err
		}
		ddl, err := base64.StdEncoding.DecodeString(comment)
		if err != nil {
			return err
		}

		ctx.SetCurrentDatabase(schemaName)
		_, iter, err := engine.Query(ctx, string(ddl))
		if err != nil {
			return err
		}
		_, err = sql.RowIterToRows(ctx, iter)
		if err != nil {
			return err
		}
	}
	return nil
}
