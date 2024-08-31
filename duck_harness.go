// Copyright 2024 ApeCloud, Inc.

// Copyright 2020-2021 Dolthub, Inc.
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
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/apecloud/myduckserver/meta"
	"github.com/dolthub/vitess/go/mysql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
)

const testNumPartitions = 5

type IndexDriverInitializer func([]sql.Database) sql.IndexDriver

type DuckHarness struct {
	name               string
	parallelism        int
	numTablePartitions int
	//	readonly                  bool
	provider                  sql.DatabaseProvider
	indexDriverInitializer    IndexDriverInitializer
	driver                    sql.IndexDriver
	nativeIndexSupport        bool
	skippedQueries            map[string]struct{}
	session                   sql.Session
	retainSession             bool
	setupData                 []setup.SetupScript
	externalProcedureRegistry sql.ExternalStoredProcedureRegistry
	server                    bool
	mu                        *sync.Mutex
}

var _ enginetest.Harness = (*DuckHarness)(nil)

// var _ enginetest.IndexDriverHarness = (*DuckHarness)(nil)
var _ enginetest.IndexHarness = (*DuckHarness)(nil)
var _ enginetest.ForeignKeyHarness = (*DuckHarness)(nil)
var _ enginetest.KeylessTableHarness = (*DuckHarness)(nil)
var _ enginetest.ClientHarness = (*DuckHarness)(nil)
var _ enginetest.ServerHarness = (*DuckHarness)(nil)
var _ sql.ExternalStoredProcedureProvider = (*DuckHarness)(nil)

func NewDuckHarness(name string, parallelism int, numTablePartitions int, useNativeIndexes bool, driverInitializer IndexDriverInitializer) *DuckHarness {
	externalProcedureRegistry := sql.NewExternalStoredProcedureRegistry()
	for _, esp := range memory.ExternalStoredProcedures {
		externalProcedureRegistry.Register(esp)
	}

	var useServer bool
	if _, ok := os.LookupEnv("SERVER_ENGINE_TEST"); ok {
		useServer = true
	}

	return &DuckHarness{
		name:                      name,
		numTablePartitions:        numTablePartitions,
		indexDriverInitializer:    driverInitializer,
		parallelism:               parallelism,
		nativeIndexSupport:        useNativeIndexes,
		skippedQueries:            make(map[string]struct{}),
		externalProcedureRegistry: externalProcedureRegistry,
		mu:                        &sync.Mutex{},
		server:                    useServer,
	}
}

func NewDefaultDuckHarness() *DuckHarness {
	return NewDuckHarness("default", 1, testNumPartitions, false, nil)
}

// func NewReadOnlyDuckHarness() *DuckHarness {
// 	h := NewDefaultDuckHarness()
// 	h.readonly = true
// 	return h
// }

func (m *DuckHarness) SessionBuilder() server.SessionBuilder {
	return func(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlConnectionUser, ok := c.UserData.(sql.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}
		client := sql.Client{Address: host, User: user, Capabilities: c.Capabilities}
		baseSession := sql.NewBaseSessionWithClientServer(addr, client, c.ConnectionID)
		return memory.NewSession(baseSession, m.getProvider()), nil
	}
}

// ExternalStoredProcedure implements the sql.ExternalStoredProcedureProvider interface
func (m *DuckHarness) ExternalStoredProcedure(_ *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	return m.externalProcedureRegistry.LookupByNameAndParamCount(name, numOfParams)
}

// ExternalStoredProcedures implements the sql.ExternalStoredProcedureProvider interface
func (m *DuckHarness) ExternalStoredProcedures(_ *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	return m.externalProcedureRegistry.LookupByName(name)
}

// func (m *DuckHarness) InitializeIndexDriver(dbs []sql.Database) {
// 	if m.indexDriverInitializer != nil {
// 		m.driver = m.indexDriverInitializer(dbs)
// 	}
// }

func (m *DuckHarness) NewSession() *sql.Context {
	m.session = m.newSession()
	return m.NewContext()
}

func (m *DuckHarness) SkipQueryTest(query string) bool {
	_, ok := m.skippedQueries[strings.ToLower(query)]
	return ok
}

func (m *DuckHarness) QueriesToSkip(queries ...string) {
	for _, query := range queries {
		m.skippedQueries[strings.ToLower(query)] = struct{}{}
	}
}

func (m *DuckHarness) UseServer() {
	m.server = true
}

func (m *DuckHarness) IsUsingServer() bool {
	return m.server
}

type SkippingDuckHarness struct {
	DuckHarness
}

var _ enginetest.SkippingHarness = (*SkippingDuckHarness)(nil)

func NewSkippingDuckHarness() *SkippingDuckHarness {
	return &SkippingDuckHarness{
		DuckHarness: *NewDefaultDuckHarness(),
	}
}

func (s SkippingDuckHarness) SkipQueryTest(query string) bool {
	return true
}

func (m *DuckHarness) Setup(setupData ...[]setup.SetupScript) {
	m.setupData = nil
	for i := range setupData {
		m.setupData = append(m.setupData, setupData[i]...)
	}
}

func (m *DuckHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	if !m.retainSession {
		m.session = nil
		m.provider = nil
	}
	engine, err := NewEngine(t, m, m.getProvider(), m.setupData, memory.NewStatsProv())
	if err != nil {
		return nil, err
	}

	if m.server {
		return enginetest.NewServerQueryEngine(t, engine, m.SessionBuilder())
	}

	return engine, nil
}

// NewEngine creates an engine and sets it up for testing using harness, provider, and setup data given.
func NewEngine(t *testing.T, harness enginetest.Harness, dbProvider sql.DatabaseProvider, setupData []setup.SetupScript, statsProvider sql.StatsProvider) (*sqle.Engine, error) {
	e := enginetest.NewEngineWithProvider(t, harness, dbProvider)
	e.Analyzer.Catalog.StatsProvider = statsProvider

	provider := dbProvider.(*meta.DbProvider)
	builder := NewDuckBuilder(e.Analyzer.ExecBuilder, provider.Storage(), provider.CatalogName())
	e.Analyzer.ExecBuilder = builder

	ctx := enginetest.NewContext(harness)

	var supportsIndexes bool
	if ih, ok := harness.(enginetest.IndexHarness); ok && ih.SupportsNativeIndexCreation() {
		supportsIndexes = true
	}

	// TODO: remove ths, make it explicit everywhere
	if len(setupData) == 0 {
		setupData = setup.MydbData
	}
	return RunSetupScripts(ctx, e, setupData, supportsIndexes)
}

// RunSetupScripts runs the given setup scripts on the given engine, returning any error
func RunSetupScripts(ctx *sql.Context, e *sqle.Engine, scripts []setup.SetupScript, createIndexes bool) (*sqle.Engine, error) {
	for i := range scripts {
		for _, s := range scripts[i] {
			if !createIndexes {
				if strings.Contains(s, "create index") || strings.Contains(s, "create unique index") {
					continue
				}
			}
			// ctx.GetLogger().Warnf("running query %s\n", s)
			ctx := ctx.WithQuery(s)
			_, iter, _, err := e.Query(ctx, s)
			if err != nil {
				return nil, err
			}
			_, err = sql.RowIterToRows(ctx, iter)
			if err != nil {
				return nil, err
			}
		}
	}
	return e, nil
}

func (m *DuckHarness) SupportsNativeIndexCreation() bool {
	return m.nativeIndexSupport
}

func (m *DuckHarness) SupportsForeignKeys() bool {
	return false
}

func (m *DuckHarness) SupportsKeylessTables() bool {
	return true
}

func (m *DuckHarness) Parallelism() int {
	return m.parallelism
}

func (m *DuckHarness) NewContext() *sql.Context {
	if m.session == nil {
		m.session = m.newSession()
	}

	return sql.NewContext(
		context.Background(),
		sql.WithSession(m.session),
	)
}

func (m *DuckHarness) newSession() *memory.Session {
	baseSession := enginetest.NewBaseSession()
	session := memory.NewSession(baseSession, m.getProvider())
	if m.driver != nil {
		session.GetIndexRegistry().RegisterIndexDriver(m.driver)
	}
	return session
}

func (m *DuckHarness) NewContextWithClient(client sql.Client) *sql.Context {
	baseSession := sql.NewBaseSessionWithClientServer("address", client, 1)

	return sql.NewContext(
		context.Background(),
		sql.WithSession(memory.NewSession(baseSession, m.getProvider())),
	)
}

func (m *DuckHarness) IndexDriver(dbs []sql.Database) sql.IndexDriver {
	if m.indexDriverInitializer != nil {
		return m.indexDriverInitializer(dbs)
	}
	return nil
}

func (m *DuckHarness) WithProvider(provider sql.DatabaseProvider) *DuckHarness {
	ret := *m
	ret.provider = provider
	return &ret
}

func (m *DuckHarness) getProvider() sql.DatabaseProvider {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.provider == nil {
		m.provider = m.NewDatabaseProvider().(*meta.DbProvider)
	}

	return m.provider
}

func (m *DuckHarness) NewDatabaseProvider() sql.MutableDatabaseProvider {
	return meta.NewInMemoryDBProvider()
}

func (m *DuckHarness) Provider() *meta.DbProvider {
	return m.getProvider().(*meta.DbProvider)
}

func (m *DuckHarness) ValidateEngine(ctx *sql.Context, e *sqle.Engine) error {
	return sanityCheckEngine(ctx, e)
}

func sanityCheckEngine(ctx *sql.Context, e *sqle.Engine) (err error) {
	for _, db := range e.Analyzer.Catalog.AllDatabases(ctx) {
		if err = sanityCheckDatabase(ctx, db); err != nil {
			return err
		}
	}
	return
}

func sanityCheckDatabase(ctx *sql.Context, db sql.Database) error {
	names, err := db.GetTableNames(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		t, ok, err := db.GetTableInsensitive(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("expected to find table %s", name)
		}
		if t.Name() != name {
			return fmt.Errorf("unexpected table name (%s !=  %s)", name, t.Name())
		}
	}
	return nil
}
