package slot

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

type SlotMeta struct {
	MetaType byte
	Children []*SlotMeta
}

type Slot interface {
	//Make the slot type to be bytes
	ToBytes() []byte

	//Read needed bytes from byte array and make itself a Slot object
	MakeSlot(buf []byte, offset uint32) uint32
}

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
func Write() {

}
