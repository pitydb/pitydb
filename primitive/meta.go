package primitive

type PType int32

const (
	PTypeByte = iota
	PTypeInt32
	PTypeUInt32
	PTypeInt64
	PTypeUInt64
	PTypeFloat32
	PTypeFloat64
	PTypeBool //1 is true , 0 is false
	PTypeString
)

type CellMeta struct {
	pos          *Int32
	name         *String
	comment      *String
	mType        *Int32
	defaultValue interface{}
}

var _indexRowMeta *RowMeta = nil

func init() {
	slot0 := NewCellMetaRaw(0, PTypeUInt32, "key", "its the key to find", 0)
	_indexRowMeta = NewRowMeta()
	_indexRowMeta.AddMetaRowItem(slot0)
}

// DefaultIndexRowMeta is the default RowMeta for IndexPageRow
// It has only one instance because it can be use more times
func DefaultIndexRowMeta() *RowMeta {
	_indexRowMeta.items[0].mType.SetValue(PTypeInt32)
	return _indexRowMeta
}

func NewCellMetaRaw(pos int, typ PType, name string, comment string, defaultValue interface{}) *CellMeta {
	tPos := NewInt32()
	tPos.SetValue(int32(pos))
	tName := NewString()
	tName.SetValue(name)
	tComment := NewString()
	tComment.SetValue(comment)
	tTyp := NewInt32()
	tTyp.SetValue(int32(typ))

	return NewCellMeta(tPos, tTyp, tName, tComment, defaultValue)
}

func NewCellMeta(pos *Int32, mType *Int32, name *String, comment *String, defaultValue interface{}) *CellMeta {
	return &CellMeta{
		pos:          pos,
		name:         name,
		comment:      comment,
		mType:        mType,
		defaultValue: defaultValue,
	}
}

func (s *CellMeta) GetPos() int {
	return int(s.pos.value)
}

func (s *CellMeta) GetName() string {
	return s.name.value
}
func (s *CellMeta) GetComment() string {
	return s.comment.value
}
func (s *CellMeta) GetMType() PType {
	return PType(s.mType.value)
}
func (s *CellMeta) GetDefaultValue() interface{} {
	return s.defaultValue
}

func (s *CellMeta) NewCell() CellHolder {
	var r CellHolder = nil
	switch PType(s.mType.value) {
	case PTypeInt32:
		r = NewInt32()
	case PTypeUInt32:
		r = NewUInt32()
	case PTypeByte:
		r = NewByte()
	case PTypeBool:
		r = NewBool()
	case PTypeInt64:
		r = NewInt64()
	case PTypeUInt64:
		r = NewUInt64()
	case PTypeFloat32:
		r = NewFloat32()
	case PTypeFloat64:
		r = NewFloat64()
	case PTypeString:
		r = NewString()
	}
	if s.defaultValue != nil {
		r.SetValue(s.defaultValue)
	}
	return r
}

type RowMeta struct {
	items []*CellMeta
}

func NewRowMeta() *RowMeta {
	return &RowMeta{
		items: make([]*CellMeta, 0),
	}
}

func (meta *RowMeta) AddMetaRowItem(item *CellMeta) {
	pos := int(item.pos.value)
	oldLen := len(meta.items)
	if pos >= oldLen {
		newItems := make([]*CellMeta, pos+1)
		if pos > 0 && oldLen > 0 {
			copy(newItems[:oldLen], meta.items)
		}
		meta.items = newItems
	}
	meta.items[pos] = item
}

func (meta *RowMeta) GetItems() []*CellMeta {
	return meta.items
}

func (meta *RowMeta) GetCellSize() int {
	return len(meta.items)
}
