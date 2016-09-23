package slot

import "github.com/lycying/pitydb/backend/fs"

const (
	ST_ROOT byte = 0x00        //root flag
	ST_INTEGER byte = 0x01
	ST_LONG byte = 0x02
	ST_FLOAT byte = 0x03
	ST_DOUBLE byte = 0x04
	ST_BOOL byte = 0x05        //byte 0 is false and byte 1 is true
	ST_STRING byte = 0x06
	ST_LIST byte = 0x07
	ST_SET byte = 0x08
	ST_HASH byte = 0x09        //Hash value that all string to string
	ST_JSON byte = 0x10        //Json str storage
	ST_BLOB byte = 0x11        //object that large than 1 page
)



type Slot interface {
	fs.Persistent
}

//NullSlot Do nothing and it's size is zero
type NullSlot struct {
	Slot
}

//Integer's value is int32
type Integer struct {
	Slot
	Val int32
}

//Long's value is int64
type Long struct {
	Slot
	Val int64
}

//Float's value is float32
type Float struct {
	Slot
	Val float32
}

//Double's value is float64
type Double struct {
	Slot
	Val float64
}

//Boolean's value is bool
type Boolean struct {
	Slot
	Val bool
}

//String's value is string
type String struct {
	Slot
	Val string
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
	}
	return NewNullSlot()
}
