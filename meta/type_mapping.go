package meta

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
)

// TODO(ysg): Refactor this implementation by using interface{} to represent a DuckDB type,
// and implement the interface{} type for each corresponding MySQL type.
// The current large mapping function is error-prone and difficult to maintain.

type duckType struct {
	str   string
	extra string // extra is only used for some types to specify the original type in the mysqlType string, e.g. "VARCHAR(255)", "DATETIME", "YEAR"
}

func newDT(str string) duckType {
	return newDuckType(str, "")
}

func newDuckType(str, extra string) duckType {
	return duckType{str: str, extra: extra}
}

func newDuckTypeLength(str, extra string, length int64) duckType {
	return newDuckType(str, fmt.Sprintf("%s(%d)", extra, length))
}

func newNumberType(str string, displayWidth int) duckType {
	return newDuckTypeLength(str, str, int64(displayWidth))
}

func newMediumIntType(displayWidth int) duckType {
	return newDuckTypeLength("INTEGER", "MEDIUMINT", int64(displayWidth))
}

func newUnsignedMediumIntType(displayWidth int) duckType {
	return newDuckTypeLength("UINTEGER", "UMEDIUMINT", int64(displayWidth))
}

func (d duckType) decodeExtra() (string, int64) {
	if d.extra == "" {
		return "", 0
	}
	extraParts := strings.Split(d.extra, "(")
	if len(extraParts) == 1 {
		// no parameters
		return d.extra, 0
	}
	if len(extraParts) != 2 {
		panic(fmt.Sprintf("invalid extra string: %s", d.extra))
	}
	lengthStr := strings.TrimSuffix(extraParts[1], ")")
	length, err := strconv.ParseInt(lengthStr, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("invalid extra string: %s, error: %v", d.extra, err))
	}
	return extraParts[0], length
}

const DuckDBDecimalTypeMaxPrecision = 38

func duckdbDataType(mysqlType sql.Type) (duckType, error) {

	// ugly ? no, AI helps us
	switch mysqlType.Type() {
	case sqltypes.Int8:
		return newNumberType("TINYINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint8:
		return newNumberType("UTINYINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Int16:
		return newNumberType("SMALLINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint16:
		return newNumberType("USMALLINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Int24:
		return newMediumIntType(mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint24:
		return newUnsignedMediumIntType(mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Int32:
		return newNumberType("INTEGER", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint32:
		return newNumberType("UINTEGER", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Int64:
		return newNumberType("BIGINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint64:
		return newNumberType("UBIGINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Float32:
		return newDT("FLOAT"), nil
	case sqltypes.Float64:
		return newDT("DOUBLE"), nil
	case sqltypes.Timestamp:
		return newDT("TIMESTAMP"), nil // TODO: check if this is correct
	case sqltypes.Date:
		return newDT("DATE"), nil
	case sqltypes.Time:
		return newDT("TIME"), nil
	case sqltypes.Datetime:
		return newDuckType("TIMESTAMP", "DATETIME"), nil
	case sqltypes.Year:
		return newDuckType("SMALLINT", "YEAR"), nil
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
		return newDT(fmt.Sprintf("DECIMAL(%d, %d)", prec, scale)), nil
	// the logic is based on https://github.com/dolthub/go-mysql-server/blob/ed8de8d3a4e6a3c3f76788821fd3890aca4806bc/sql/types/strings.go#L570
	case sqltypes.Text:
		return newDuckTypeLength("VARCHAR", "TEXT", mysqlType.(sql.StringType).MaxByteLength()), nil
	case sqltypes.Blob:
		return newDuckTypeLength("BLOB", "BLOB", mysqlType.(sql.StringType).MaxByteLength()), nil
	case sqltypes.VarChar:
		return newDuckTypeLength("VARCHAR", "VARCHAR", mysqlType.(sql.StringType).MaxCharacterLength()), nil
	case sqltypes.VarBinary:
		return newDuckTypeLength("BLOB", "VARBINARY", mysqlType.(sql.StringType).MaxCharacterLength()), nil
	case sqltypes.Char:
		return newDuckTypeLength("VARCHAR", "CHAR", mysqlType.(sql.StringType).MaxCharacterLength()), nil
	case sqltypes.Binary:
		return newDuckTypeLength("BLOB", "BINARY", mysqlType.(sql.StringType).MaxCharacterLength()), nil
	case sqltypes.Bit:
		return newDuckTypeLength("BIT", "BIT", int64(mysqlType.(types.BitType).NumberOfBits())), nil
	case sqltypes.TypeJSON:
		return newDT("JSON"), nil // TODO: install json extension in DuckDB
	case sqltypes.Enum, sqltypes.Set, sqltypes.Geometry, sqltypes.Expression:
		return newDT(""), fmt.Errorf("unsupported MySQL type: %s", mysqlType.String())
	default:
		panic(fmt.Sprintf("encountered unknown MySQL type(%v). This is likely a bug - please check the duckdbDataType function for missing type mappings", mysqlType.Type()))
	}
}

func mysqlDataType(duckdbType duckType, numericPrecision uint8, numericScale uint8) sql.Type {
	// TODO: The current type mappings are not lossless. We need to store the original type in the column comments.
	duckdbTypeStr := strings.TrimSpace(strings.ToUpper(duckdbType.str))

	if strings.HasPrefix(duckdbTypeStr, "DECIMAL") {
		duckdbTypeStr = "DECIMAL"
	}

	myType, length := duckdbType.decodeExtra()

	// process integer types, they all have displayWidth
	intBaseType := sqltypes.Null
	switch duckdbTypeStr {
	case "TINYINT":
		intBaseType = sqltypes.Int8
	case "UTINYINT":
		intBaseType = sqltypes.Uint8
	case "SMALLINT":
		{
			if myType == "YEAR" {
				return types.Year
			}
			intBaseType = sqltypes.Int16
		}
	case "USMALLINT":
		intBaseType = sqltypes.Uint16
	case "INTEGER":
		{
			if myType == "MEDIUMINT" {
				intBaseType = sqltypes.Int24
			} else {
				intBaseType = sqltypes.Int32
			}
		}
	case "UINTEGER":
		{
			if myType == "UMEDIUMINT" {
				intBaseType = sqltypes.Uint24
			} else {
				intBaseType = sqltypes.Uint32
			}
		}
	case "BIGINT":
		intBaseType = sqltypes.Int64
	case "UBIGINT":
		intBaseType = sqltypes.Uint64
	}

	if intBaseType != sqltypes.Null {
		return types.MustCreateNumberTypeWithDisplayWidth(intBaseType, int(length))
	}

	switch duckdbTypeStr {
	case "FLOAT":
		return types.Float32
	case "DOUBLE":
		return types.Float64
	case "TIMESTAMP":
		{
			if myType == "DATETIME" {
				return types.Datetime
			}
			return types.Timestamp
		}
	case "DATE":
		return types.Date
	case "TIME":
		return types.Time
	case "DECIMAL":
		return types.MustCreateDecimalType(numericPrecision, numericScale)
	case "VARCHAR":
		{
			if myType == "TEXT" {
				if length <= types.TinyTextBlobMax {
					return types.TinyText
				} else if length <= types.TextBlobMax {
					return types.Text
				} else if length <= types.MediumTextBlobMax {
					return types.MediumText
				} else {
					return types.LongText
				}
			} else if myType == "VARCHAR" {
				return types.MustCreateStringWithDefaults(sqltypes.VarChar, length)
			} else if myType == "CHAR" {
				return types.MustCreateStringWithDefaults(sqltypes.Char, length)
			}
			return types.Text
		}
	case "BLOB":
		{

			if myType == "BLOB" {
				if length <= types.TinyTextBlobMax {
					return types.TinyBlob
				} else if length <= types.TextBlobMax {
					return types.Blob
				} else if length <= types.MediumTextBlobMax {
					return types.MediumBlob
				} else {
					return types.LongBlob
				}
			} else if myType == "VARBINARY" {
				return types.MustCreateBinary(sqltypes.VarBinary, length)
			} else if myType == "BINARY" {
				return types.MustCreateBinary(sqltypes.Binary, length)
			}
			return types.Blob
		}
	case "BIT":
		{
			if myType == "BIT" {
				return types.MustCreateBitType(uint8(length))
			}
			return types.MustCreateBitType(types.BitTypeMaxBits)
		}
	case "JSON":
		return types.JSON
	default:
		panic(fmt.Sprintf("encountered unknown DuckDB type(%s). This is likely a bug - please check the duckdbDataType function for missing type mappings", duckdbType))
	}
}
