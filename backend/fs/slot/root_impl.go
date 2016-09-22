package slot



func NewRoot(meta *SlotMeta) *Root {
	return &Root{Meta:meta}
}
func (r *Root) ToBytes() []byte {
	buf := make([]byte, 0)
	for _, it := range r.Val {
		b := it.ToBytes()
		buf = append(buf, b...)
	}
	return buf
}

func (r *Root) MakeSlot(buf []byte, offset uint32) uint32 {
	if r.Meta.MetaType != ST_ROOT {
		return 0
	}
	idx := offset
	for _, it := range r.Meta.Children {
		slot := MakeSlot(it.MetaType)
		n := slot.MakeSlot(buf, idx)
		r.Val = append(r.Val, slot)
		idx += n
	}
	return idx
}
