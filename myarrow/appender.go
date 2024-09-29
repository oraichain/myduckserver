package myarrow

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/shopspring/decimal"
)

type ArrowAppender struct {
	*array.RecordBuilder
}

func NewArrowAppender(schema sql.Schema, dictionary ...int) (ArrowAppender, error) {
	pool := memory.NewGoAllocator()
	arrowSchema, err := ToArrowSchema(schema, dictionary...)
	if err != nil {
		return ArrowAppender{}, err
	}
	return ArrowAppender{array.NewRecordBuilder(pool, arrowSchema)}, nil
}

// Build creates a new arrow.Record from the memory buffers and resets the builder.
// The returned Record must be Release()'d after use.
func (a *ArrowAppender) Build() arrow.Record {
	return a.RecordBuilder.NewRecord()
}

func (a *ArrowAppender) Append(row sql.Row) error {
	for i, b := range a.RecordBuilder.Fields() {
		v := row[i]
		if v == nil {
			b.AppendNull()
			continue
		}
		switch b.Type().ID() {
		case arrow.UINT8:
			b.(*array.Uint8Builder).Append(v.(uint8))
		case arrow.INT8:
			b.(*array.Int8Builder).Append(v.(int8))
		case arrow.UINT16:
			b.(*array.Uint16Builder).Append(v.(uint16))
		case arrow.INT16:
			b.(*array.Int16Builder).Append(v.(int16))
		case arrow.UINT32:
			b.(*array.Uint32Builder).Append(v.(uint32))
		case arrow.INT32:
			b.(*array.Int32Builder).Append(v.(int32))
		case arrow.UINT64:
			b.(*array.Uint64Builder).Append(v.(uint64))
		case arrow.INT64:
			b.(*array.Int64Builder).Append(v.(int64))
		case arrow.FLOAT32:
			b.(*array.Float32Builder).Append(v.(float32))
		case arrow.FLOAT64:
			b.(*array.Float64Builder).Append(v.(float64))
		case arrow.STRING:
			b.(*array.StringBuilder).Append(v.(string))
		case arrow.BINARY:
			b.(*array.BinaryBuilder).Append(v.([]byte))
		case arrow.DECIMAL:
			dv := v.(decimal.Decimal)
			b.AppendValueFromString(dv.String())
		case arrow.TIMESTAMP:
			tv := v.(time.Time)
			at, err := arrow.TimestampFromTime(tv, b.Type().(*arrow.TimestampType).Unit)
			if err != nil {
				return err
			}
			b.(*array.TimestampBuilder).Append(at)
		case arrow.DATE32:
			tv := v.(time.Time)
			b.(*array.Date32Builder).Append(arrow.Date32FromTime(tv))
		case arrow.DURATION:
			sv := v.(string)
			duration, err := types.Time.ConvertToTimeDuration(sv)
			if err != nil {
				return err
			}
			b.(*array.DurationBuilder).Append(arrow.Duration(duration.Microseconds()))
		case arrow.DICTIONARY:
			switch v := v.(type) {
			case string:
				b.(*array.BinaryDictionaryBuilder).AppendString(v)
			case []byte:
				b.(*array.BinaryDictionaryBuilder).Append(v)
			default:
				b.AppendValueFromString(fmt.Sprint(v))
			}
		default:
			b.AppendValueFromString(fmt.Sprint(v))
		}
	}
	return nil
}
