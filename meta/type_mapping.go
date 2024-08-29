package meta

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
)

const DuckDBDecimalTypeMaxPrecision = 38

func duckdbDataType(mysqlType sql.Type) (string, error) {
	switch mysqlType.Type() {
	case sqltypes.Int8:
		return "TINYINT", nil
	case sqltypes.Uint8:
		return "UTINYINT", nil
	case sqltypes.Int16:
		return "SMALLINT", nil
	case sqltypes.Uint16:
		return "USMALLINT", nil
	case sqltypes.Int24:
		return "INTEGER", nil
	case sqltypes.Uint24:
		return "UINTEGER", nil
	case sqltypes.Int32:
		return "INTEGER", nil
	case sqltypes.Uint32:
		return "UINTEGER", nil
	case sqltypes.Int64:
		return "BIGINT", nil
	case sqltypes.Uint64:
		return "UBIGINT", nil
	case sqltypes.Float32:
		return "FLOAT", nil
	case sqltypes.Float64:
		return "DOUBLE", nil
	case sqltypes.Timestamp:
		return "TIMESTAMP", nil // TODO: check if this is correct
	case sqltypes.Date:
		return "DATE", nil
	case sqltypes.Time:
		return "TIME", nil
	case sqltypes.Datetime:
		return "TIMESTAMP", nil
	case sqltypes.Year:
		return "SMALLINT", nil
	case sqltypes.Decimal:
		decimal := mysqlType.(sql.DecimalType)
		prec := decimal.Precision()
		scale := decimal.Scale()
		// truncate precision to max supported by DuckDB
		if prec > DuckDBDecimalTypeMaxPrecision {
			prec = DuckDBDecimalTypeMaxPrecision
			// scale must be less than or equal to precision
			if scale > prec {
				scale = prec
			}
		}
		return fmt.Sprintf("DECIMAL(%d, %d)", prec, scale), nil
	case sqltypes.Text:
		return "VARCHAR", nil
	case sqltypes.Blob:
		return "BLOB", nil
	case sqltypes.VarChar:
		return "VARCHAR", nil
	case sqltypes.VarBinary:
		return "BLOB", nil
	case sqltypes.Char:
		return "CHAR", nil
	case sqltypes.Binary:
		return "BLOB", nil
	case sqltypes.Bit:
		return "BIT", nil
	case sqltypes.TypeJSON:
		return "JSON", nil // TODO: install json extension in DuckDB
	case sqltypes.Enum, sqltypes.Set, sqltypes.Geometry, sqltypes.Expression:
		return "", fmt.Errorf("unsupported MySQL type: %s", mysqlType.String())
	default:
		panic(fmt.Sprintf("encountered unknown MySQL type(%v). This is likely a bug - please check the duckdbDataType function for missing type mappings", mysqlType.Type()))
	}
}

func mysqlDataType(duckdbType string, numericPrecision uint8, numericScale uint8) sql.Type {
	// TODO: The current type mappings are not lossless. We need to store the original type in the column comments.
	duckdbType = strings.TrimSpace(strings.ToUpper(duckdbType))

	if strings.HasPrefix(duckdbType, "DECIMAL") {
		duckdbType = "DECIMAL"
	}

	switch duckdbType {
	case "TINYINT":
		return types.Int8
	case "UTINYINT":
		return types.Uint8
	case "SMALLINT":
		return types.Int16
	case "USMALLINT":
		return types.Uint16
	case "INTEGER":
		return types.Int32
	case "UINTEGER":
		return types.Uint32
	case "BIGINT":
		return types.Int64
	case "UBIGINT":
		return types.Uint64
	case "FLOAT":
		return types.Float32
	case "DOUBLE":
		return types.Float64
	case "TIMESTAMP":
		return types.Timestamp
	case "DATE":
		return types.Date
	case "TIME":
		return types.Time
	case "DECIMAL":
		return types.MustCreateDecimalType(numericPrecision, numericScale)
	case "VARCHAR":
		return types.Text
	case "BLOB":
		return types.Blob
	case "CHAR":
		return types.Text
	case "BIT":
		return types.MustCreateBitType(types.BitTypeMaxBits)
	case "JSON":
		return types.JSON
	default:
		panic(fmt.Sprintf("encountered unknown DuckDB type(%s). This is likely a bug - please check the duckdbDataType function for missing type mappings", duckdbType))
	}
}
