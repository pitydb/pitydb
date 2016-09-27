package slot

import "github.com/lycying/pitydb/backend/fs"

const (
	ST_ROOT byte = iota//root flag
	ST_BYTE
	ST_INTEGER
	ST_UNSIGNED_INTEGER
	ST_LONG
	ST_UNSIGNED_LONG
	ST_FLOAT
	ST_DOUBLE
	ST_BOOL         //byte 0 is false and byte 1 is true
	ST_STRING
	ST_LIST
	ST_SET
	ST_HASH        //Hash value that all string to string
	ST_JSON        //Json str storage
	ST_BLOB        //object that large than 1 page
)

type Slot interface {
	fs.Persistent
	Len() uint32
}

//NullSlot Do nothing and it's size is zero
type NullSlot struct {
	Slot
}

//Integer's value is int32
type Integer struct {
	Slot
	Value int32
}

//Unsigned Integer's value is int32
type UnsignedInteger struct {
	Slot
	Value uint32
}

//Long's value is int64
type Long struct {
	Slot
	Value int64
}
//Long's value is int64
type UnsignedLong struct {
	Slot
	Value uint64
}
//Float's value is float32
type Float struct {
	Slot
	Value float32
}

//Double's value is float64
type Double struct {
	Slot
	Value float64
}

//Boolean's value is bool
type Boolean struct {
	Slot
	Value bool
}

//String's value is string
type String struct {
	Slot
	Value string
}

type Byte struct {
	Slot
	Value byte
}

//Make a new Slot via the byte value, if nothing found ,return NullSlot
func MakeSlot(typ byte) Slot {
	switch typ{
	case ST_INTEGER:
		return NewInteger(0)
	case ST_LONG:
		return NewLong(0)
	case ST_FLOAT:
		return NewFloat(0)
	case ST_DOUBLE:
		return NewDouble(0)
	case ST_BOOL:
		return NewBoolean(false)
	case ST_STRING:
		return NewString("")
	case ST_UNSIGNED_INTEGER:
		return NewUnsignedInteger(0)
	case ST_UNSIGNED_LONG:
		return NewUnsignedLong(0)
	case ST_BYTE:
		return NewByte(0x00)
	}
	return NewNullSlot()
}
