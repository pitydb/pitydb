package slot

import (
	"testing"
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"bytes"
)

//try to make a new bytes and add some random bytes at header
//to see if can make it the real string object and get the real bytes
func TestStringSlot_MakeSlot_ToBytes(t *testing.T) {
	v := NewString("")
	space := make([]byte, 11)
	b1 := make([]byte, 4)
	b2 := "hello world!!!你?星星✨12345"
	size := []byte(b2)
	binary.BigEndian.PutUint32(b1, uint32(len(size)))
	tmp := append(b1, size...)
	arr := append(space, tmp...)
	v.Make(arr, uint32(11))

	assert.Equal(t, v.Val, b2, "they should be equals")
	assert.Equal(t, v.ToBytes(), append(b1, size...), "they should be equals")
}

func TestIntSlot_MakeSlot_ToBytes(t *testing.T) {
	v := NewInteger(0)
	value := int32(-2147483648)
	space := make([]byte, 1001)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, &value)

	v.Make(append(space, buf.Bytes()...), 1001)

	assert.Equal(t, v.Val, value, "they should be equals")
	assert.Equal(t, v.ToBytes(), buf.Bytes(), "they should be equals")

}
func TestLongSlot_MakeSlot_ToBytes(t *testing.T) {
	v := NewLong(0)
	value := int64(9223372036854775807)
	space := make([]byte, 1234)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, &value)

	v.Make(append(space, buf.Bytes()...), 1234)

	assert.Equal(t, v.Val, value, "they should be equals")
	assert.Equal(t, v.ToBytes(), buf.Bytes(), "they should be equals")

}

func TestFloatSlot_MakeSlot_ToBytes(t *testing.T) {
	v := NewFloat(0)
	value := float32(3.141592653589793323432424234234242)
	space := make([]byte, 4321)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, &value)

	v.Make(append(space, buf.Bytes()...), 4321)

	assert.Equal(t, v.Val, value, "they should be equals", v)
	assert.Equal(t, v.ToBytes(), buf.Bytes(), "they should be equals")

}

func TestBooleanSlot_MakeSlot_ToBytes(t *testing.T) {
	v := NewBoolean(false)
	value := true
	space := make([]byte, 4321)

	v.Make(append(space, 0x01), 4321)

	assert.Equal(t, v.Val, value, "they should be equals", v)
	assert.Equal(t, v.ToBytes(), []byte{0x01}, "they should be equals")

}

