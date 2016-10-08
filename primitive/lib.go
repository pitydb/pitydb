package primitive

import "unsafe"

// A structPointer is a pointer to a struct.
type structPointer unsafe.Pointer


// Marshaler is the interface representing objects that can marshal themselves.
type Marshaler interface {
	Marshal() ([]byte, error)
}

// Unmarshaler is the interface representing objects that can
// unmarshal themselves.  The method should reset the receiver before
// decoding starts.  The argument points to data that may be
// overwritten, so implementations should not keep references to the
// buffer.
type Unmarshaler interface {
	Unmarshal(buf []byte, offset int) (int, error)
}

type Byte struct {
	value byte
}
type Bool struct {
	value bool
}

type Int32 struct {
	value int32
}

type Int struct {
	value int32
}

type Int64 struct {
	value int64
}

type Float32 struct {
	value float32
}

type Float64 struct {
	value float64
}

type Uint32 struct {
	value uint32
}

type Uint64 struct {
	value uint32
}

type String struct {
	value string
}
