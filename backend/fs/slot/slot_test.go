package slot

import (
	"testing"
	"encoding/binary"
)

func TestStringSlot_MakeSlot(t *testing.T) {
	v := &StringSlot{}
	space := make([]byte, 11)
	b1 := make([]byte, 4)
	b2 := "hello world!!!你?星星✨12345"
	size := []byte(b2)
	binary.BigEndian.PutUint32(b1, uint32(len(size)))
	tmp := append(b1, size...)
	arr := append(space, tmp...)
	v.MakeSlot(arr, uint32(11))
	println(v.Val == b2, v.Val)
}