package catalog

import "strings"

type InternalTable struct {
	Name         string
	KeyColumns   []string
	ValueColumns []string
	DDL          string
}

func (it *InternalTable) QualifiedName() string {
	return "main." + it.Name
}

func (it *InternalTable) UpsertStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("INSERT OR REPLACE INTO main.")
	b.WriteString(it.Name)
	b.WriteString(" VALUES (?")
	for range it.KeyColumns[1:] {
		b.WriteString(", ?")
	}
	for range it.ValueColumns {
		b.WriteString(", ?")
	}
	b.WriteString(")")
	return b.String()
}

func (it *InternalTable) DeleteStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("DELETE FROM main.")
	b.WriteString(it.Name)
	b.WriteString(" WHERE ")
	b.WriteString(it.KeyColumns[0])
	b.WriteString(" = ?")
	for _, c := range it.KeyColumns[1:] {
		b.WriteString(c)
		b.WriteString(" = ?")
	}
	return b.String()
}

func (it *InternalTable) SelectStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("SELECT ")
	b.WriteString(it.ValueColumns[0])
	for _, c := range it.ValueColumns[1:] {
		b.WriteString(", ")
		b.WriteString(c)
	}
	b.WriteString(" FROM main.")
	b.WriteString(it.Name)
	b.WriteString(" WHERE ")
	b.WriteString(it.KeyColumns[0])
	b.WriteString(" = ?")
	for _, c := range it.KeyColumns[1:] {
		b.WriteString(" AND ")
		b.WriteString(c)
		b.WriteString(" = ?")
	}
	return b.String()
}

var InternalTables = struct {
	PersistentVariable InternalTable
	BinlogPosition     InternalTable
}{
	PersistentVariable: InternalTable{
		Name:         "persistent_variable",
		KeyColumns:   []string{"name"},
		ValueColumns: []string{"value", "vtype"},
		DDL:          "name TEXT PRIMARY KEY, value TEXT, vtype TEXT",
	},
	BinlogPosition: InternalTable{
		Name:         "binlog_position",
		KeyColumns:   []string{"channel"},
		ValueColumns: []string{"position"},
		DDL:          "channel TEXT PRIMARY KEY, position TEXT",
	},
}

var internalTables = []InternalTable{
	InternalTables.PersistentVariable,
	InternalTables.BinlogPosition,
}
