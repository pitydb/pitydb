package primitive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

const ()

type PrimitiveValue interface {
}

func NewInt32() *Int32 {
	return &Int32{}
}
func NewUInt32() *UInt32 {
	return &UInt32{}
}
func NewInt64() *Int64 {
	return &Int64{}
}
func NewUInt64() *UInt64 {
	return &UInt64{}
}
func NewFloat32() *Float32 {
	return &Float32{}
}
func NewFloat64() *Float64 {
	return &Float64{}
}
func NewBool() *Bool {
	return &Bool{value: false}
}
func NewByte() *Byte {
	return &Byte{}
}
func NewString() *String {
	return &String{value: ""}
}

func (h *Bool) Encode() ([]byte, error) {
	var b byte = 0x0
	if h.value {
		b = 0x1
	}
	return []byte{b}, nil
}

func (h *Bool) Decode(buf []byte, offset int) (int, error) {
	b := buf[offset]
	h.value = (b == 0x1)
	return 1, nil
}

func (h *Bool) SetValue(v PrimitiveValue) {
	h.value = v.(bool)
}

func (h *Bool) GetValue() PrimitiveValue {
	return h.value
}

func (h *Bool) GetLen() int {
	return 1
}

func (h *Byte) Encode() ([]byte, error) {
	return []byte{h.value}, nil
}

func (h *Byte) Decode(buf []byte, offset int) (int, error) {
	b := buf[offset]
	h.value = b
	return 1, nil
}

func (h *Byte) SetValue(v PrimitiveValue) {
	h.value = v.(byte)
}

func (h *Byte) GetValue() PrimitiveValue {
	return h.value
}

func (h *Byte) GetLen() int {
	return 1
}

func (h *String) Encode() ([]byte, error) {
	var retArr []byte
	var lenFlag byte
	var offset int

	strArr := []byte(h.value)
	strLen := len(strArr)

	switch {
	case strLen > math.MaxUint32:
		return nil, errors.New("string too long")
	case strLen > math.MaxUint16:
		retArr = make([]byte, 1+4+strLen)
		binary.BigEndian.PutUint32(retArr[1:5], uint32(strLen))
		lenFlag = 0x3
		offset = 5
	case strLen > math.MaxUint8:
		retArr = make([]byte, 1+2+strLen)
		binary.BigEndian.PutUint16(retArr[1:3], uint16(strLen))
		lenFlag = 0x2
		offset = 3
	case strLen > 0:
		retArr = make([]byte, 1+1+strLen)
		retArr[1] = byte(strLen)
		lenFlag = 0x1
		offset = 2
	case strLen == 0:
		retArr = make([]byte, 1)
		lenFlag = 0x0
		offset = 1
	}
	retArr[0] = lenFlag
	if strLen > 0 {
		copy(retArr[offset:], strArr)
	}
	return retArr, nil
}

func (h *String) Decode(buf []byte, offset int) (int, error) {
	lenFlag := buf[offset]
	retLen := 1

	offset = offset + 1 // the retLen has 1 byte
	switch lenFlag {
	case 0x0:
		h.value = ""
	case 0x1:
		size := int(buf[offset])
		h.value = string(buf[offset+1 : offset+1+size])
		retLen = retLen + 1 + size
	case 0x2:
		size := int(binary.BigEndian.Uint16(buf[offset : offset+2]))
		h.value = string(buf[offset+2 : offset+2+size])
		retLen = retLen + 2 + size
	case 0x3:
		size := int(binary.BigEndian.Uint32(buf[offset : offset+4]))
		h.value = string(buf[offset+4 : offset+4+size])
		retLen = retLen + 4 + size
	}

	return retLen, nil
}

func (h *String) SetValue(v PrimitiveValue) {
	h.value = v.(string)
}

func (h *String) GetValue() PrimitiveValue {
	return h.value
}

func (h *String) GetLen() int {
	strLen := len(h.value)
	switch {
	case strLen > math.MaxUint16:
		return 1 + 4 + strLen
	case strLen > math.MaxUint8:
		return 1 + 2 + strLen
	case strLen > 0:
		return 1 + 1 + strLen
	case strLen == 0:
		return 1
	}
	return 0 // never reached
}

func (h *Int32) Encode() ([]byte, error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(h.value))
	return buf, nil
}

func (h *Int32) Decode(buf []byte, offset int) (int, error) {
	h.value = int32(binary.BigEndian.Uint32(buf[offset : offset+4]))
	return 4, nil
}

func (h *Int32) SetValue(v PrimitiveValue) {
	h.value = v.(int32)
}

func (h *Int32) GetValue() PrimitiveValue {
	return h.value
}

func (h *Int32) GetLen() int {
	return 4
}

func (h *UInt32) Encode() ([]byte, error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, h.value)
	return buf, nil
}

func (h *UInt32) Decode(buf []byte, offset int) (int, error) {
	h.value = binary.BigEndian.Uint32(buf[offset : offset+4])
	return 4, nil
}

func (h *UInt32) SetValue(v PrimitiveValue) {
	h.value = v.(uint32)
}

func (h *UInt32) GetValue() PrimitiveValue {
	return h.value
}

func (h *UInt32) GetLen() int {
	return 4
}

func (h *Int64) Encode() ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(h.value))
	return buf, nil
}

func (h *Int64) Decode(buf []byte, offset int) (int, error) {
	h.value = int64(binary.BigEndian.Uint64(buf[offset : offset+8]))
	return 8, nil
}

func (h *Int64) SetValue(v PrimitiveValue) {
	h.value = v.(int64)
}

func (h *Int64) GetValue() PrimitiveValue {
	return h.value
}

func (h *Int64) GetLen() int {
	return 8
}

func (h *UInt64) Encode() ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, h.value)
	return buf, nil
}

func (h *UInt64) Decode(buf []byte, offset int) (int, error) {
	h.value = binary.BigEndian.Uint64(buf[offset : offset+8])
	return 8, nil
}

func (h *UInt64) SetValue(v PrimitiveValue) {
	h.value = v.(uint64)
}

func (h *UInt64) GetValue() PrimitiveValue {
	return h.value
}

func (h *UInt64) GetLen() int {
	return 8
}

func (h *Float32) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, h.value)

	return buf.Bytes(), nil
}

func (h *Float32) Decode(buf []byte, offset int) (int, error) {
	byteBuf := bytes.NewReader(buf[offset : offset+4])
	binary.Read(byteBuf, binary.BigEndian, &h.value)
	return 4, nil
}

func (h *Float32) SetValue(v PrimitiveValue) {
	h.value = v.(float32)
}

func (h *Float32) GetValue() PrimitiveValue {
	return h.value
}

func (h *Float32) GetLen() int {
	return 4
}

func (h *Float64) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, h.value)

	return buf.Bytes(), nil
}

func (h *Float64) Decode(buf []byte, offset int) (int, error) {
	byteBuf := bytes.NewReader(buf[offset : offset+8])
	binary.Read(byteBuf, binary.BigEndian, &h.value)
	return 8, nil
}

func (h *Float64) SetValue(v PrimitiveValue) {
	h.value = v.(float64)
}

func (h *Float64) GetValue() PrimitiveValue {
	return h.value
}

func (h *Float64) GetLen() int {
	return 8
}
