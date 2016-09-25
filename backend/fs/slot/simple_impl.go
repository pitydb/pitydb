package slot

import (
	"encoding/binary"
	"bytes"
)

func NewNullSlot() *NullSlot {
	return &NullSlot{}
}
func NewInteger(val int32) *Integer {
	return &Integer{Val:val}
}
func NewUnsignedInteger(val uint32) *UnsignedInteger {
	return &UnsignedInteger{Val:val}
}
func NewLong(val int64) *Long {
	return &Long{Val:val}
}
func NewUnsignedLong(val uint64) *UnsignedLong {
	return &UnsignedLong{Val:val}
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
func NewByte(val byte) *Byte {
	return &Byte{Val:val}
}
func NewString(val string) *String {
	return &String{Val:val }
}
func (s *NullSlot) ToBytes() []byte {
	return nil
}

func (s *NullSlot) Make(buf []byte, offset uint32) uint32 {
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
func (s *String) Make(buf []byte, offset uint32) uint32 {
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

func (s *Integer) Make(buf []byte, offset uint32) uint32 {
	byteBuf := bytes.NewBuffer(buf[offset:offset + 4])
	binary.Read(byteBuf, binary.BigEndian, &s.Val)
	return 4
}
func (s *UnsignedInteger) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, s.Val)
	return buf.Bytes()
}

func (s *UnsignedInteger) Make(buf []byte, offset uint32) uint32 {
	byteBuf := bytes.NewBuffer(buf[offset:offset + 4])
	binary.Read(byteBuf, binary.BigEndian, &s.Val)
	return 4
}

func (s *Long) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, s.Val)
	return buf.Bytes()
}

func (s *Long) Make(buf []byte, offset uint32) uint32 {
	byteBuf := bytes.NewBuffer(buf[offset:offset + 8])
	binary.Read(byteBuf, binary.BigEndian, &s.Val)
	return 8
}
func (s *UnsignedLong) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, s.Val)
	return buf.Bytes()
}

func (s *UnsignedLong) Make(buf []byte, offset uint32) uint32 {
	byteBuf := bytes.NewBuffer(buf[offset:offset + 8])
	binary.Read(byteBuf, binary.BigEndian, &s.Val)
	return 8
}

func (s *Float) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, s.Val)
	return buf.Bytes()
}

func (s *Float) Make(buf []byte, offset uint32) uint32 {
	byteBuf := bytes.NewReader(buf[offset:offset + 4])
	binary.Read(byteBuf, binary.BigEndian, &s.Val)
	return 4
}
func (s *Double) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, s.Val)
	return buf.Bytes()
}

func (s *Double) Make(buf []byte, offset uint32) uint32 {
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

func (s *Boolean) Make(buf []byte, offset uint32) uint32 {
	s.Val = buf[offset] == 0x01
	return 1
}
func (s *Byte) ToBytes() []byte {
	return []byte{s.Val}
}

func (s *Byte) Make(buf []byte, offset uint32) uint32 {
	s.Val = buf[offset]
	return 1
}
