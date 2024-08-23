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
	stdsql "database/sql"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.RowIter = (*SQLRowIter)(nil)

// SQLRowIter wraps a standard sql.Rows as a RowIter.
type SQLRowIter struct {
	rows   *stdsql.Rows
	schema sql.Schema
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
func (iter *SQLRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if !iter.rows.Next() {
		if err := iter.rows.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	columns, err := iter.rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := iter.rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// Prune the values to match the schema
	if len(values) > len(iter.schema) {
		values = values[:len(iter.schema)]
	} else if len(values) < len(iter.schema) {
		for i := len(values); i < len(iter.schema); i++ {
			values = append(values, nil)
		}
	}

	return sql.NewRow(values...), nil
}

// Close closes the underlying sql.Rows.
func (iter *SQLRowIter) Close(ctx *sql.Context) error {
	return iter.rows.Close()
}
