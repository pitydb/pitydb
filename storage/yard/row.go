package yard

import (
	"bytes"
	prm "github.com/lycying/pitydb/primitive"
)

type RowHolder interface {
	prm.Encoder
	prm.DeCoder

	GetLen() int
}
type Row struct {
	RowHolder

	meta  *prm.RowMeta     //meta data for loop data
	key   *prm.UInt32      //the key used for b+ tree
	items []prm.CellHolder //the data part
}

func NewRow(meta *prm.RowMeta) *Row {
	return &Row{
		meta:  meta,
		key:   prm.NewUInt32(),
		items: make([]prm.CellHolder, 0),
	}
}

func (r *Row) WithDefaultValues() {
	idx := int(0)
	data := make([]prm.CellHolder, r.meta.GetCellSize())
	for i, item := range r.meta.GetItems() {
		cell := item.NewCell()
		if nil != item.GetDefaultValue() {
			cell.SetValue(item.GetDefaultValue())
		}
		data[i] = cell
		idx += cell.GetLen()
	}
	r.items = data
}

func (r *Row) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	for _, item := range r.items {
		b, _ := item.Encode()
		buf.Write(b)
	}
	return buf.Bytes(), nil
}

func (r *Row) Decode(buf []byte, offset int) (int, error) {
	idx := int(0)
	data := make([]prm.CellHolder, r.meta.GetCellSize())

	for i, item := range r.meta.GetItems() {
		cell := item.NewCell()
		cLen, _ := cell.Decode(buf, idx+offset)
		data[i] = cell
		idx += cLen
	}
	r.items = data
	return idx, nil
}

func (r *Row) GetLen() int {
	cl := 0
	for _, item := range r.items {
		cl += item.GetLen()
	}
	return cl
}

func (r *Row) GetCellAt(meta *prm.CellMeta) prm.CellHolder {
	return r.items[meta.GetPos()]
}
