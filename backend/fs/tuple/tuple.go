package tuple

import (
	"github.com/lycying/laladb/backend/fs/slot"
)

type Tuple struct {
	PreTuple  *Tuple
	NextTuple *Tuple
	Orig      *slot.Root
}

func ReadTuple(meta slot.SlotMeta, buf []byte, offset uint32) (*Tuple, int) {
	root := slot.NewRoot(meta)
	n := root.MakeSlot(buf, offset)

	return &Tuple{
		Orig:root,
	}, n
}

func ReadTupleLinked(meta slot.SlotMeta, buf []byte, offset uint32) *Tuple {
	idx := offset
	t, n := ReadTuple(meta, buf, idx)
	idx += n
	if t.Orig.Next != 0 {
		t, n = ReadTuple(meta, buf, idx)
		idx += n
	}
	return t
}
func (t *Tuple) ReadPreTuple() {
}
func (t *Tuple) ReadNextTuple() {

}
