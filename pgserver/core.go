// Copyright 2024 Dolthub, Inc.
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
package pgserver

import (
	"strings"

	"github.com/apecloud/myduckserver/backend"
	"github.com/dolthub/go-mysql-server/sql"
)

// GetSqlDatabaseFromContext returns the database from the context. Uses the context's current database if an empty
// string is provided. Returns nil if the database was not found.
func GetSqlDatabaseFromContext(ctx *sql.Context, database string) (sql.Database, error) {
	session := ctx.Session.(*backend.Session)
	if len(database) == 0 {
		database = ctx.GetCurrentDatabase()
	}
	ctx.GetLogger().Tracef("Getting database from context: %v", database)
	db, err := session.Provider().Database(ctx, database)
	if err != nil {
		if sql.ErrDatabaseNotFound.Is(err) {
			return nil, nil
		}
		return nil, err
	}
	return db, nil
}

// GetSqlTableFromContext returns the table from the context. Uses the context's current database if an empty database
// name is provided. Returns nil if no table was found.
// TODO(fan): Support search_path.
func GetSqlTableFromContext(ctx *sql.Context, databaseName string, tableName string) (sql.Table, error) {
	db, err := GetSqlDatabaseFromContext(ctx, databaseName)
	if err != nil || db == nil {
		return nil, err
	}

	tbl, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(tableName)
	}
	return tbl, nil
}

// SearchPath returns all the schemas in the search_path setting, with elements like "$user" expanded
func SearchPath(ctx *sql.Context) ([]string, error) {
	searchPathVar, err := ctx.GetSessionVariable(ctx, "search_path")
	if err != nil {
		return nil, err
	}

	pathElems := strings.Split(searchPathVar.(string), ",")
	path := make([]string, len(pathElems))
	for i, pathElem := range pathElems {
		path[i] = normalizeSearchPathSchema(ctx, pathElem)
	}

	return path, nil
}

func normalizeSearchPathSchema(ctx *sql.Context, schemaName string) string {
	schemaName = strings.Trim(schemaName, " ")
	if schemaName == "\"$user\"" {
		client := ctx.Session.Client()
		return client.User
	}
	return schemaName
}
