package row

import (
	"github.com/lycying/pitydb/backend/fs/slot"
	"github.com/lycying/pitydb/backend/fs"
)

type RowMeta struct {
	Type     byte
	Children []*RowMeta
}
type Row struct {
	fs.Persistent
	Meta         *RowMeta              //meta data for loop data

	ClusteredKey *slot.UnsignedInteger //the key used for b+ tree

	Data         []slot.Slot           //the data part
}

func NewRow(meta *RowMeta) *Row {
	return &Row{
		Meta:meta,
		ClusteredKey:slot.NewUnsignedInteger(0),
	}
}
func (r *Row) ToBytes() []byte {
	buf := make([]byte, 0)
	for _, it := range r.Data {
		b := it.ToBytes()
		buf = append(buf, b...)
	}
	return buf
}

func (r *Row) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	for _, it := range r.Meta.Children {
		s := slot.MakeSlot(it.Type)
		n := s.Make(buf, idx + offset)
		r.Data = append(r.Data, s)
		idx += n
	}
	return idx
}

func (r *Row) Fill(slots ...slot.Slot) {
	for _, v := range slots {
		r.Data = append(r.Data, v)
	}
}
