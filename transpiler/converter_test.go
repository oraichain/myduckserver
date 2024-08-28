package transpiler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Test converting MySQL strings to Postgres strings
func TestNormalizeStrings(t *testing.T) {
	type test struct {
		input    string
		expected string
	}
	tests := []test{
		{
			input:    "SELECT \"foo\" FROM `bar`",
			expected: `SELECT 'foo' FROM "bar"`,
		},
		{
			input:    `SELECT "foo"`,
			expected: `SELECT 'foo'`,
		},
		{
			input:    `SELECT "fo\"o"`,
			expected: `SELECT 'fo"o'`,
		},
		{
			input:    `SELECT "fo\'o"`,
			expected: `SELECT 'fo''o'`,
		},
		{
			input:    `SELECT 'fo\'o'`,
			expected: `SELECT 'fo''o'`,
		},
		{
			input:    `SELECT 'fo\"o'`,
			expected: `SELECT 'fo"o'`,
		},
		{
			input:    `SELECT 'fo\\"o'`,
			expected: `SELECT 'fo\"o'`,
		},
		{
			input:    `SELECT 'fo\\\'o'`,
			expected: `SELECT 'fo\''o'`,
		},
		{
			input:    `SELECT "fo\\'o"`,
			expected: `SELECT 'fo\''o'`,
		},
		{
			input:    `SELECT "fo\\\"o"`,
			expected: `SELECT 'fo\"o'`,
		},
		{
			input:    "SELECT 'fo''o'",
			expected: `SELECT 'fo''o'`,
		},
		{
			input:    "SELECT 'fo''''o'",
			expected: `SELECT 'fo''''o'`,
		},
		{
			input:    `SELECT "fo'o"`,
			expected: `SELECT 'fo''o'`,
		},
		{
			input:    `SELECT "fo''o"`,
			expected: `SELECT 'fo''''o'`,
		},
		{
			input:    `SELECT "fo""o"`,
			expected: `SELECT 'fo"o'`,
		},
		{
			input:    `SELECT "fo""""o"`,
			expected: `SELECT 'fo""o'`,
		},
		{
			input:    `SELECT 'fo""o'`,
			expected: `SELECT 'fo""o'`,
		},
		{
			input:    "SELECT `foo` FROM `bar`",
			expected: `SELECT "foo" FROM "bar"`,
		},
		{
			input:    "SELECT 'foo' FROM `bar`",
			expected: `SELECT 'foo' FROM "bar"`,
		},
		{
			input:    "SELECT `f\"o'o` FROM `ba``r`",
			expected: "SELECT \"f\"o'o\" FROM \"ba`r\"",
		},
		{
			input:    "SELECT \"foo\" from `bar` where `bar`.`baz` = \"qux\"",
			expected: `SELECT 'foo' from "bar" where "bar"."baz" = 'qux'`,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			actual := normalizeStrings(test.input)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestConvertQuery(t *testing.T) {
	type test struct {
		input    string
		expected []string
	}
	tests := []test{
		{
			input:    "CREATE TABLE foo (a INT primary key)",
			expected: []string{"CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY)"},
		},
		{
			input: "CREATE TABLE foo (a INT primary key, b int not null)",
			expected: []string{
				"CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY, b INTEGER NOT NULL)",
			},
		},
		{
			input: "CREATE TABLE foo (a INT primary key, b int, key (b))",
			expected: []string{
				"CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY, b INTEGER NULL)",
				"CREATE INDEX ON foo ( b ASC ) NULLS NOT DISTINCT ",
			},
		},
		{
			input: "CREATE TABLE foo (a INT primary key, b int, c int, key (c,b))",
			expected: []string{
				"CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY, b INTEGER NULL, c INTEGER NULL)",
				"CREATE INDEX ON foo ( c ASC, b ASC ) NULLS NOT DISTINCT ",
			},
		},
		{
			input: "CREATE TABLE foo (a INT primary key, b int, c int not null, d int, key (c), key (b), key (b,c))",
			expected: []string{
				"CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY, b INTEGER NULL, c INTEGER NOT NULL, d INTEGER NULL)",
				"CREATE INDEX ON foo ( c ASC ) NULLS NOT DISTINCT ",
				"CREATE INDEX ON foo ( b ASC ) NULLS NOT DISTINCT ",
				"CREATE INDEX ON foo ( b ASC, c ASC ) NULLS NOT DISTINCT ",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			actual := ConvertQuery(test.input)
			require.Equal(t, test.expected, actual)
		})
	}
}
