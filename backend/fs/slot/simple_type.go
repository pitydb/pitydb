package slot

import (
	"encoding/binary"
)

type NullSlot struct {
	SlotType
}

type StringSlot struct {
	SlotType
	Val string
}

func (n *NullSlot) ToBytes() []byte {
	return nil
}

func (n *NullSlot) MakeSlot(buf []byte, offset uint32) uint32 {
	return 0
}

//string length + string  bytes
func (s *StringSlot) ToBytes() []byte {
	valArray := []byte(s.Val)
	lenArray := make([]byte, 4)
	binary.BigEndian.PutUint32(lenArray, uint32(len(valArray)))
	return append(lenArray, valArray...)
}
//read length first , then read the data
//TODO uinit32 is too big to save , make it smaller
func (s *StringSlot) MakeSlot(buf []byte, offset uint32) uint32 {
	size := binary.BigEndian.Uint32(buf[offset:offset + 4])
	arr := buf[offset + 4:offset + 4 + size]
	s.Val = string(arr)
	return size
}
