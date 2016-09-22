package slot

import (
	"encoding/binary"
	"bytes"
)

type NullSlot struct {
	Slot
}
type Integer struct {
	Slot
	Val int32
}
type Long struct {
	Slot
	Val int64
}
type Float struct {
	Slot
	Val float32
}
type Double struct {
	Slot
	Val float64
}
type Boolean struct {
	Slot
	Val bool
}
type String struct {
	Slot
	Val string
}

func NewNullSlot() *NullSlot{
	return &NullSlot{}
}
func NewInteger(val int32) *Integer {
	return &Integer{Val:val}
}
func NewLong(val int64) *Long {
	return &Long{Val:val}
}
func NewFloat(val float32) *Float {
	return &Float{Val:val}
}
func NewDouble(val float64) *Double {
	return &Double{Val:val}
}
func NewBoolean(val bool) *Boolean {
	return &Boolean{Val:val}
}
func NewString(val string) *String {
	return &String{Val:val }
}
func (s *NullSlot) ToBytes() []byte {
	return nil
}

func (s *NullSlot) MakeSlot(buf []byte, offset uint32) uint32 {
	return 0
}

//string length + string  bytes
func (s *String) ToBytes() []byte {
	valArray := []byte(s.Val)
	lenArray := make([]byte, 4)
	binary.BigEndian.PutUint32(lenArray, uint32(len(valArray)))
	return append(lenArray, valArray...)
}

//read length first , then read the data
//TODO uinit32 is too big to save , make it smaller
func (s *String) MakeSlot(buf []byte, offset uint32) uint32 {
	size := uint32(binary.BigEndian.Uint32(buf[offset:offset + 4]))
	arr := buf[offset + 4:offset + 4 + size]
	s.Val = string(arr)
	return size + 4
}

func (s *Integer) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, s.Val)
	return buf.Bytes()
}

func (s *Integer) MakeSlot(buf []byte, offset uint32) uint32 {
	byteBuf := bytes.NewBuffer(buf[offset:offset + 4])
	binary.Read(byteBuf, binary.BigEndian, &s.Val)
	return 4
}

func (s *Long) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, s.Val)
	return buf.Bytes()
}

func (s *Long) MakeSlot(buf []byte, offset uint32) uint32 {
	byteBuf := bytes.NewBuffer(buf[offset:offset + 8])
	binary.Read(byteBuf, binary.BigEndian, &s.Val)
	return 8
}

func (s *Float) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, s.Val)
	return buf.Bytes()
}

func (s *Float) MakeSlot(buf []byte, offset uint32) uint32 {
	byteBuf := bytes.NewReader(buf[offset:offset + 4])
	binary.Read(byteBuf, binary.BigEndian, &s.Val)
	return 4
}
func (s *Double) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, s.Val)
	return buf.Bytes()
}

func (s *Double) MakeSlot(buf []byte, offset uint32) uint32 {
	byteBuf := bytes.NewReader(buf[offset:offset + 8])
	binary.Read(byteBuf, binary.BigEndian, &s.Val)
	return 8
}
func (s *Boolean) ToBytes() []byte {
	if s.Val {
		return []byte{0x01}
	}else {
		return []byte{0x00}
	}
}

func (s *Boolean) MakeSlot(buf []byte, offset uint32) uint32 {
	s.Val = buf[offset] == 0x01
	return 1
}
