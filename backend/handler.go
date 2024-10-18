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

package backend

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/vitess/go/mysql"
)

type MyHandler struct {
	*server.Handler
	pool *ConnectionPool
}

// Precompile regex for performance
var autoIncrementRegex = regexp.MustCompile(`AUTO_INCREMENT=\d+`)

func (h *MyHandler) ConnectionClosed(c *mysql.Conn) {
	h.pool.CloseConn(c.ConnectionID)
	h.Handler.ConnectionClosed(c)
}

func (h *MyHandler) ComInitDB(c *mysql.Conn, schemaName string) error {
	_, err := h.pool.GetConnForSchema(context.Background(), c.ConnectionID, schemaName)
	if err != nil {
		return err
	}
	return h.Handler.ComInitDB(c, schemaName)
}

// Naive query rewriting. This is just a temporary solution
// and should be replaced with a more robust implementation.
func (h *MyHandler) ComQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	callback mysql.ResultSpoolFn,
) error {
	// https://github.com/dolthub/dolt/issues/8455
	query = strings.ReplaceAll(query, "CHARACTER SET 'utf8mb4'", "CHARACTER SET utf8mb4")

	query = autoIncrementRegex.ReplaceAllString(query, "")

	return h.Handler.ComQuery(ctx, c, query, callback)
}

func WrapHandler(pool *ConnectionPool) server.HandlerWrapper {
	return func(h mysql.Handler) (mysql.Handler, error) {
		handler, ok := h.(*server.Handler)
		if !ok {
			return nil, fmt.Errorf("expected *server.Handler, got %T", h)
		}

		return &MyHandler{
			Handler: handler,
			pool:    pool,
		}, nil
	}
}
