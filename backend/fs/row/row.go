package row

import (
	"github.com/lycying/pitydb/backend/fs/slot"
	"github.com/lycying/pitydb/backend/fs"
)

type RowMeta struct {
	Type     byte
	Children []*RowMeta
}
type RowDef struct {
	fs.Persistent

	Pre    uint32      //the pre root
	Next   uint32      //the next root
	PageID uint32      //the page id
	Key    uint32      //the key used for b+ tree
	Meta   *RowMeta    //meta data for loop data
	Val    []slot.Slot //the data part
}

func NewRowDef(meta *RowMeta) *RowDef {
	return &RowDef{Meta:meta}
}
func (r *RowDef) ToBytes() []byte {
	buf := make([]byte, 0)
	for _, it := range r.Val {
		b := it.ToBytes()
		buf = append(buf, b...)
	}
	return buf
}

func (r *RowDef) Make(buf []byte, offset uint32) uint32 {
	idx := offset
	for _, it := range r.Meta.Children {
		s := slot.MakeSlot(it.Type)
		n := s.Make(buf, idx)
		r.Val = append(r.Val, s)
		idx += n
	}
	return idx
}

type Tuple struct {
	PreTuple  *Tuple
	NextTuple *Tuple
	Orig      *RowDef
}

func ReadTuple(meta *RowMeta, buf []byte, offset uint32) (*Tuple, uint32) {
	r := NewRowDef(meta)
	n := r.Make(buf, offset)

	return &Tuple{
		Orig:r,
	}, n
}

func ReadTupleLinked(meta *RowMeta, buf []byte, offset uint32) *Tuple {
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
