package catalog

import (
	"strings"
)

func FullSchemaName(catalog, schema string) string {
	if catalog == "" {
		return `"` + schema + `"`
	}
	// why?
	if schema == "" {
		return `"` + catalog + `"`
	}
	return `"` + catalog + `"."` + schema + `"`
}

func FullTableName(catalog, schema, table string) string {
	return FullSchemaName(catalog, schema) + `."` + table + `"`
}

func FullIndexName(catalog, schema, index string) string {
	return FullTableName(catalog, schema, index)
}

func FullColumnName(catalog, schema, table, column string) string {
	return FullTableName(catalog, schema, table) + `."` + column + `"`
}

// EncodeIndexName uses a simple encoding scheme (table$$index) for better visibility which is useful for debugging.
func EncodeIndexName(table, index string) string {
	return table + "$$" + index
}

func DecodeIndexName(encodedName string) (string, string) {
	parts := strings.Split(encodedName, "$$")
	// without "$$", the encodedName is the index name
	if len(parts) != 2 {
		return "", encodedName
	}
	return parts[0], parts[1]
}

// DecodeCreateindex extracts column names from a SQL string, Only consider single-column indexes or multi-column indexes
// TODO: using sqlparser to parse columns name, now identifiers(index name, table name, column name) cannot include parentheses.
// such as CREATE INDEX "idx((())hello" ON db.T((t.a)); will cause an error
func DecodeCreateindex(createIndexSQL string) []string {
	leftParen := strings.Index(createIndexSQL, "(")
	rightParen := strings.Index(createIndexSQL, ")")
	if leftParen != -1 && rightParen != -1 {
		content := createIndexSQL[leftParen+1 : rightParen]
		columns := strings.Split(content, ",")
		for i, col := range columns {
			columns[i] = strings.TrimSpace(col)
		}
		return columns
	}
	return []string{}
}

func QuoteIdentifierANSI(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func ConnectIdentifiersANSI(identifiers ...string) string {
	for i, id := range identifiers {
		identifiers[i] = QuoteIdentifierANSI(id)
	}
	return strings.Join(identifiers, ".")
}
