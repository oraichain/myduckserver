package catalog

import "strings"

type MacroDefinition struct {
	Params []string
	DDL    string
}

var (
	SchemaNameSYS           string = "__sys__"
	MacroNameMyListContains string = "my_list_contains"

	MacroNameMySplitListStr string = "my_split_list_str"
)

type InternalMacro struct {
	Schema       string
	Name         string
	IsTableMacro bool
	// A macro can be overloaded with multiple definitions, each with a different set of parameters.
	// https://duckdb.org/docs/sql/statements/create_macro.html#overloading
	Definitions []MacroDefinition
}

func (v *InternalMacro) QualifiedName() string {
	if strings.ToLower(v.Schema) == "pg_catalog" {
		return "__sys__." + v.Name
	}
	return v.Schema + "." + v.Name
}

var InternalMacros = []InternalMacro{
	{
		Schema:       "information_schema",
		Name:         "_pg_expandarray",
		IsTableMacro: true,
		Definitions: []MacroDefinition{
			{
				Params: []string{"a"},
				DDL: `SELECT STRUCT_PACK(
    x := unnest(a),
    n := generate_series(1, array_length(a))
) AS item`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_get_indexdef",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"index_oid"},
				// Do nothing currently
				DDL: `''`,
			},
			{
				Params: []string{"index_oid", "column_no", "pretty_bool"},
				// Do nothing currently
				DDL: `''`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_get_expr",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"pg_node_tree", "relation_oid"},
				// Do nothing currently
				DDL: `pg_catalog.pg_get_expr(pg_node_tree, relation_oid)`,
			},
			{
				Params: []string{"pg_node_tree", "relation_oid", "pretty_bool"},
				// Do nothing currently
				DDL: `pg_catalog.pg_get_expr(pg_node_tree, relation_oid)`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_get_partkeydef",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"oid"},
				// Return a placeholder text for partition key definition
				DDL: `'PARTITION BY (not_implemented)'::text`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_relation_size",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"oid"},
				// Return a placeholder size value for relation size
				DDL: `0::bigint`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_stat_get_numscans",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"oid"},
				// Return a placeholder value for number of scans
				DDL: `0::bigint`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_encoding_to_char",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"integer"},
				// Return a placeholder encoding name
				DDL: `'UTF8'::VARCHAR`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_tablespace_location",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"oid"},
				// Return a placeholder tablespace location
				DDL: `''::VARCHAR`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_get_keywords",
		IsTableMacro: true,
		Definitions: []MacroDefinition{
			{
				Params: []string{},
				// Return a comprehensive list of SQL keywords
				DDL: `SELECT 
    'SELECT'::VARCHAR AS word,
    'R'::VARCHAR AS catcode,
    'reserved'::VARCHAR AS catdesc
UNION ALL SELECT 'FROM', 'R', 'reserved'
UNION ALL SELECT 'WHERE', 'R', 'reserved'
UNION ALL SELECT 'INSERT', 'R', 'reserved'
UNION ALL SELECT 'UPDATE', 'R', 'reserved'
UNION ALL SELECT 'DELETE', 'R', 'reserved'
UNION ALL SELECT 'CREATE', 'R', 'reserved'
UNION ALL SELECT 'DROP', 'R', 'reserved'
UNION ALL SELECT 'ALTER', 'R', 'reserved'
UNION ALL SELECT 'TABLE', 'R', 'reserved'
UNION ALL SELECT 'INDEX', 'R', 'reserved'
UNION ALL SELECT 'VIEW', 'R', 'reserved'
UNION ALL SELECT 'SCHEMA', 'R', 'reserved'
UNION ALL SELECT 'DATABASE', 'R', 'reserved'
UNION ALL SELECT 'USER', 'R', 'reserved'
UNION ALL SELECT 'ROLE', 'R', 'reserved'
UNION ALL SELECT 'GRANT', 'R', 'reserved'
UNION ALL SELECT 'REVOKE', 'R', 'reserved'
UNION ALL SELECT 'COMMIT', 'R', 'reserved'
UNION ALL SELECT 'ROLLBACK', 'R', 'reserved'
UNION ALL SELECT 'BEGIN', 'R', 'reserved'
UNION ALL SELECT 'END', 'R', 'reserved'
UNION ALL SELECT 'TRANSACTION', 'R', 'reserved'
UNION ALL SELECT 'SAVEPOINT', 'R', 'reserved'
UNION ALL SELECT 'EXPLAIN', 'R', 'reserved'
UNION ALL SELECT 'ANALYZE', 'R', 'reserved'
UNION ALL SELECT 'VACUUM', 'R', 'reserved'
UNION ALL SELECT 'REINDEX', 'R', 'reserved'
UNION ALL SELECT 'CLUSTER', 'R', 'reserved'
UNION ALL SELECT 'CHECKPOINT', 'R', 'reserved'
UNION ALL SELECT 'PREPARE', 'R', 'reserved'
UNION ALL SELECT 'EXECUTE', 'R', 'reserved'
UNION ALL SELECT 'DEALLOCATE', 'R', 'reserved'
UNION ALL SELECT 'LISTEN', 'R', 'reserved'
UNION ALL SELECT 'NOTIFY', 'R', 'reserved'
UNION ALL SELECT 'UNLISTEN', 'R', 'reserved'
UNION ALL SELECT 'LOAD', 'R', 'reserved'
UNION ALL SELECT 'COPY', 'R', 'reserved'
UNION ALL SELECT 'LOCK', 'R', 'reserved'
UNION ALL SELECT 'UNLOCK', 'R', 'reserved'
UNION ALL SELECT 'RESET', 'R', 'reserved'
UNION ALL SELECT 'SHOW', 'R', 'reserved'
UNION ALL SELECT 'SET', 'R', 'reserved'
UNION ALL SELECT 'TRUNCATE', 'R', 'reserved'
UNION ALL SELECT 'DISCARD', 'R', 'reserved'
UNION ALL SELECT 'REASSIGN', 'R', 'reserved'
UNION ALL SELECT 'REFRESH', 'R', 'reserved'
UNION ALL SELECT 'SECURITY', 'R', 'reserved'
UNION ALL SELECT 'LABEL', 'R', 'reserved'
UNION ALL SELECT 'IMPORT', 'R', 'reserved'
UNION ALL SELECT 'EXPORT', 'R', 'reserved'
UNION ALL SELECT 'ATTACH', 'R', 'reserved'
UNION ALL SELECT 'DETACH', 'R', 'reserved'
UNION ALL SELECT 'BACKUP', 'R', 'reserved'
UNION ALL SELECT 'RESTORE', 'R', 'reserved'
UNION ALL SELECT 'CHECK', 'R', 'reserved'
UNION ALL SELECT 'REPAIR', 'R', 'reserved'
UNION ALL SELECT 'OPTIMIZE', 'R', 'reserved'
UNION ALL SELECT 'CACHE', 'R', 'reserved'
UNION ALL SELECT 'FLUSH', 'R', 'reserved'
UNION ALL SELECT 'KILL', 'R', 'reserved'
UNION ALL SELECT 'SIGNAL', 'R', 'reserved'
UNION ALL SELECT 'RESIGNAL', 'R', 'reserved'
UNION ALL SELECT 'ITERATE', 'R', 'reserved'
UNION ALL SELECT 'LEAVE', 'R', 'reserved'
UNION ALL SELECT 'LOOP', 'R', 'reserved'
UNION ALL SELECT 'REPEAT', 'R', 'reserved'
UNION ALL SELECT 'UNTIL', 'R', 'reserved'
UNION ALL SELECT 'WHILE', 'R', 'reserved'
UNION ALL SELECT 'CASE', 'R', 'reserved'
UNION ALL SELECT 'WHEN', 'R', 'reserved'
UNION ALL SELECT 'THEN', 'R', 'reserved'
UNION ALL SELECT 'ELSE', 'R', 'reserved'
UNION ALL SELECT 'IF', 'R', 'reserved'
UNION ALL SELECT 'RETURN', 'R', 'reserved'
UNION ALL SELECT 'CALL', 'R', 'reserved'
UNION ALL SELECT 'DECLARE', 'R', 'reserved'
UNION ALL SELECT 'HANDLER', 'R', 'reserved'
UNION ALL SELECT 'CONDITION', 'R', 'reserved'
UNION ALL SELECT 'CURSOR', 'R', 'reserved'
UNION ALL SELECT 'CONTINUE', 'R', 'reserved'
UNION ALL SELECT 'EXIT', 'R', 'reserved'
UNION ALL SELECT 'GOTO', 'R', 'reserved'
UNION ALL SELECT 'PROCEDURE', 'R', 'reserved'
UNION ALL SELECT 'FUNCTION', 'R', 'reserved'
UNION ALL SELECT 'TRIGGER', 'R', 'reserved'
UNION ALL SELECT 'EVENT', 'R', 'reserved'
UNION ALL SELECT 'TABLESPACE', 'R', 'reserved'
UNION ALL SELECT 'SEQUENCE', 'R', 'reserved'
UNION ALL SELECT 'DOMAIN', 'R', 'reserved'
UNION ALL SELECT 'TYPE', 'R', 'reserved'
UNION ALL SELECT 'AGGREGATE', 'R', 'reserved'
UNION ALL SELECT 'OPERATOR', 'R', 'reserved'
UNION ALL SELECT 'CAST', 'R', 'reserved'
UNION ALL SELECT 'CONVERT', 'R', 'reserved'
UNION ALL SELECT 'TRANSLATE', 'R', 'reserved'
UNION ALL SELECT 'TRANSLATION', 'R', 'reserved'
UNION ALL SELECT 'COLLATION', 'R', 'reserved'
UNION ALL SELECT 'CONSTRAINT', 'R', 'reserved'
UNION ALL SELECT 'FOREIGN', 'R', 'reserved'
UNION ALL SELECT 'PRIMARY', 'R', 'reserved'
UNION ALL SELECT 'UNIQUE', 'R', 'reserved'
UNION ALL SELECT 'CHECK', 'R', 'reserved'
UNION ALL SELECT 'DEFAULT', 'R', 'reserved'
UNION ALL SELECT 'NULL', 'R', 'reserved'
UNION ALL SELECT 'NOT', 'R', 'reserved'
UNION ALL SELECT 'AND', 'R', 'reserved'
UNION ALL SELECT 'OR', 'R', 'reserved'
UNION ALL SELECT 'XOR', 'R', 'reserved'
UNION ALL SELECT 'IS', 'R', 'reserved'
UNION ALL SELECT 'IN', 'R', 'reserved'
UNION ALL SELECT 'LIKE', 'R', 'reserved'
UNION ALL SELECT 'REGEXP', 'R', 'reserved'
UNION ALL SELECT 'RLIKE', 'R', 'reserved'
UNION ALL SELECT 'SOUNDS', 'R', 'reserved'
UNION ALL SELECT 'BETWEEN', 'R', 'reserved'
UNION ALL SELECT 'EXISTS', 'R', 'reserved'
UNION ALL SELECT 'ALL', 'R', 'reserved'
UNION ALL SELECT 'ANY', 'R', 'reserved'
UNION ALL SELECT 'SOME', 'R', 'reserved'
UNION ALL SELECT 'DISTINCT', 'R', 'reserved'
UNION ALL SELECT 'AS', 'R', 'reserved'
UNION ALL SELECT 'ASC', 'R', 'reserved'
UNION ALL SELECT 'DESC', 'R', 'reserved'
UNION ALL SELECT 'ORDER', 'R', 'reserved'
UNION ALL SELECT 'BY', 'R', 'reserved'
UNION ALL SELECT 'GROUP', 'R', 'reserved'
UNION ALL SELECT 'HAVING', 'R', 'reserved'
UNION ALL SELECT 'LIMIT', 'R', 'reserved'
UNION ALL SELECT 'OFFSET', 'R', 'reserved'
UNION ALL SELECT 'UNION', 'R', 'reserved'
UNION ALL SELECT 'INTERSECT', 'R', 'reserved'
UNION ALL SELECT 'EXCEPT', 'R', 'reserved'
UNION ALL SELECT 'MINUS', 'R', 'reserved'
UNION ALL SELECT 'JOIN', 'R', 'reserved'
UNION ALL SELECT 'INNER', 'R', 'reserved'
UNION ALL SELECT 'LEFT', 'R', 'reserved'
UNION ALL SELECT 'RIGHT', 'R', 'reserved'
UNION ALL SELECT 'FULL', 'R', 'reserved'
UNION ALL SELECT 'OUTER', 'R', 'reserved'
UNION ALL SELECT 'CROSS', 'R', 'reserved'
UNION ALL SELECT 'NATURAL', 'R', 'reserved'
UNION ALL SELECT 'ON', 'R', 'reserved'
UNION ALL SELECT 'USING', 'R', 'reserved'
UNION ALL SELECT 'WITH', 'R', 'reserved'
UNION ALL SELECT 'RECURSIVE', 'R', 'reserved'
UNION ALL SELECT 'WINDOW', 'R', 'reserved'
UNION ALL SELECT 'OVER', 'R', 'reserved'
UNION ALL SELECT 'PARTITION', 'R', 'reserved'
UNION ALL SELECT 'RANGE', 'R', 'reserved'
UNION ALL SELECT 'ROWS', 'R', 'reserved'
UNION ALL SELECT 'GROUPS', 'R', 'reserved'
UNION ALL SELECT 'UNBOUNDED', 'R', 'reserved'
UNION ALL SELECT 'PRECEDING', 'R', 'reserved'
UNION ALL SELECT 'FOLLOWING', 'R', 'reserved'
UNION ALL SELECT 'CURRENT', 'R', 'reserved'
UNION ALL SELECT 'ROW', 'R', 'reserved'
UNION ALL SELECT 'LAG', 'R', 'reserved'
UNION ALL SELECT 'LEAD', 'R', 'reserved'
UNION ALL SELECT 'FIRST_VALUE', 'R', 'reserved'
UNION ALL SELECT 'LAST_VALUE', 'R', 'reserved'
UNION ALL SELECT 'NTH_VALUE', 'R', 'reserved'
UNION ALL SELECT 'NTILE', 'R', 'reserved'
UNION ALL SELECT 'RANK', 'R', 'reserved'
UNION ALL SELECT 'DENSE_RANK', 'R', 'reserved'
UNION ALL SELECT 'PERCENT_RANK', 'R', 'reserved'
UNION ALL SELECT 'CUME_DIST', 'R', 'reserved'
UNION ALL SELECT 'PERCENTILE_CONT', 'R', 'reserved'
UNION ALL SELECT 'PERCENTILE_DISC', 'R', 'reserved'`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "string_to_array",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"input_string", "delimiter"},
				// Convert PostgreSQL array string to DuckDB array
				DDL: `regexp_split_to_array(input_string, delimiter)`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_table_is_visible",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"oid"},
				// Return true for table visibility (simplified)
				DDL: `true::boolean`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_get_userbyid",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"oid"},
				// Return a placeholder username
				DDL: `'postgres'::VARCHAR`,
			},
		},
	},
	{
		Schema:       SchemaNameSYS,
		Name:         MacroNameMyListContains,
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"l", "v"},
				DDL: `CASE
    WHEN typeof(l) = 'VARCHAR' THEN
        list_contains(regexp_split_to_array(l::VARCHAR, '[{},\s]+'), v)
    ELSE
        list_contains(l::text[], v)
    END`,
			},
		},
	},
	{
		Schema:       SchemaNameSYS,
		Name:         MacroNameMySplitListStr,
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"l"},
				DDL:    `regexp_split_to_array(l::VARCHAR, '[{},\s]+')`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "current_schemas",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"boolean"},
				// Return the current schemas based on the boolean parameter
				// For now, return a simple array with 'public' schema
				DDL: `ARRAY['public']::VARCHAR[]`,
			},
		},
	},
}
