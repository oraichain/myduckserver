package pgserver

import (
	stdsql "database/sql"
	"fmt"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

var defaultTypeMap = pgtype.NewMap()

var duckdbToPostgresTypeMap = map[string]string{
	"INVALID":      "unknown",
	"BOOLEAN":      "bool",
	"TINYINT":      "int2",
	"SMALLINT":     "int2",
	"INTEGER":      "int4",
	"BIGINT":       "int8",
	"UTINYINT":     "int2",    // Unsigned tinyint, approximated to int2
	"USMALLINT":    "int4",    // Unsigned smallint, approximated to int4
	"UINTEGER":     "int8",    // Unsigned integer, approximated to int8
	"UBIGINT":      "numeric", // Unsigned bigint, approximated to numeric for large values
	"FLOAT":        "float4",
	"DOUBLE":       "float8",
	"TIMESTAMP":    "timestamp",
	"DATE":         "date",
	"TIME":         "time",
	"INTERVAL":     "interval",
	"HUGEINT":      "numeric",
	"UHUGEINT":     "numeric",
	"VARCHAR":      "text",
	"BLOB":         "bytea",
	"DECIMAL":      "numeric",
	"TIMESTAMP_S":  "timestamp",
	"TIMESTAMP_MS": "timestamp",
	"TIMESTAMP_NS": "timestamp",
	"ENUM":         "text",
	"UUID":         "uuid",
	"BIT":          "bit",
	"TIME_TZ":      "timetz",
	"TIMESTAMP_TZ": "timestamptz",
	"ANY":          "text",    // Generic ANY type approximated to text
	"VARINT":       "numeric", // Variable integer, mapped to numeric
}

func inferSchema(rows *stdsql.Rows) (sql.Schema, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	schema := make(sql.Schema, len(types))
	for i, t := range types {
		pgTypeName, ok := duckdbToPostgresTypeMap[t.DatabaseTypeName()]
		if !ok {
			return nil, fmt.Errorf("unsupported type %s", t.DatabaseTypeName())
		}
		pgType, ok := defaultTypeMap.TypeForName(pgTypeName)
		if !ok {
			return nil, fmt.Errorf("unsupported type %s", pgTypeName)
		}
		nullable, _ := t.Nullable()
		schema[i] = &sql.Column{
			Name: t.Name(),
			Type: PostgresType{
				ColumnType: t,
				PG:         pgType,
			},
			Nullable: nullable,
		}
	}

	return schema, nil
}

type PostgresType struct {
	*stdsql.ColumnType
	PG *pgtype.Type
}

func (p PostgresType) Encode(v any, buf []byte) ([]byte, error) {
	return defaultTypeMap.Encode(p.PG.OID, pgproto3.TextFormat, v, buf)
}

var _ sql.Type = PostgresType{}

func (p PostgresType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	panic("not implemented")
}

func (p PostgresType) Compare(v1 interface{}, v2 interface{}) (int, error) {
	panic("not implemented")
}

func (p PostgresType) Convert(v interface{}) (interface{}, sql.ConvertInRange, error) {
	panic("not implemented")
}

func (p PostgresType) Equals(t sql.Type) bool {
	panic("not implemented")
}

func (p PostgresType) MaxTextResponseByteLength(_ *sql.Context) uint32 {
	panic("not implemented")
}

func (p PostgresType) Promote() sql.Type {
	panic("not implemented")
}

func (p PostgresType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	panic("not implemented")
}

func (p PostgresType) Type() query.Type {
	panic("not implemented")
}

func (p PostgresType) ValueType() reflect.Type {
	panic("not implemented")
}

func (p PostgresType) Zero() interface{} {
	panic("not implemented")
}

func (p PostgresType) String() string {
	panic("not implemented")
}
