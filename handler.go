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
	"fmt"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

type MyHandler struct {
	*server.Handler
	builder *DuckBuilder
}

func (h *MyHandler) ConnectionClosed(c *mysql.Conn) {
	conn, ok := h.builder.conns[c.ConnectionID]
	if ok {
		if err := conn.Close(); err != nil {
			logrus.Warn("Failed to close connection:", err)
		}
		delete(h.builder.conns, c.ConnectionID)
	}
	h.Handler.ConnectionClosed(c)
}

func (h *MyHandler) ComInitDB(c *mysql.Conn, schemaName string) error {
	conn, ok := h.builder.conns[c.ConnectionID]
	if ok {
		if _, err := conn.Exec("USE " + dbName + "." + schemaName); err != nil {
			logrus.Warn("Failed to use "+schemaName+":", err)
		}
	}
	return h.Handler.ComInitDB(c, schemaName)
}

func wrapHandler(b *DuckBuilder) server.HandlerWrapper {
	return func(h mysql.Handler) (mysql.Handler, error) {
		handler, ok := h.(*server.Handler)
		if !ok {
			return nil, fmt.Errorf("expected *server.Handler, got %T", h)
		}

		return &MyHandler{
			Handler: handler,
			builder: b,
		}, nil
	}
}
