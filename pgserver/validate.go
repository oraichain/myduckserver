// Copyright 2024 Dolthub, Inc.
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

package pgserver

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

// Validate returns an error if the CopyFrom node is invalid, for example if it contains columns that
// are not in the table schema.
func ValidateCopyFrom(cf *tree.CopyFrom, ctx *sql.Context) error {
	table, err := GetSqlTableFromContext(ctx, cf.Table.Schema(), cf.Table.Table())
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf(`relation "%s" does not exist`, cf.Table.Table())
	}
	if _, ok := table.(sql.InsertableTable); !ok {
		return fmt.Errorf(`table "%s" is read-only`, cf.Table.Table())
	}

	// If a set of columns was explicitly specified, validate them
	if len(cf.Columns) > 0 {
		if len(table.Schema()) != len(cf.Columns) {
			return fmt.Errorf("invalid column name list for table %s: %v", table.Name(), cf.Columns)
		}

		for i, col := range table.Schema() {
			name := cf.Columns[i]
			nameString := strings.Trim(name.String(), `"`)
			if nameString != col.Name {
				return fmt.Errorf("invalid column name list for table %s: %v", table.Name(), cf.Columns)
			}
		}
	}

	return nil
}
