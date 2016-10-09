package primitive

type Encoder interface {
	Encode() ([]byte, error)
}

type DeCoder interface {
	Decode(buf []byte, offset int) (int, error)
}

type CellHolder interface {
	Encoder
	DeCoder

	SetValue(PrimitiveValue)
	GetValue() PrimitiveValue
	GetLen() int
}

type Byte struct {
	CellHolder
	value byte
}
type Bool struct {
	CellHolder
	value bool
}

type Int32 struct {
	CellHolder
	value int32
}

type Int64 struct {
	CellHolder
	value int64
}

type Float32 struct {
	CellHolder
	value float32
}

type Float64 struct {
	CellHolder
	value float64
}

type UInt32 struct {
	CellHolder
	value uint32
}

type UInt64 struct {
	CellHolder
	value uint64
}

type String struct {
	CellHolder
	value string
}
