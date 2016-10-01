package page

import (
	"github.com/lycying/pitydb/backend/fs"
	"github.com/lycying/pitydb/backend/fs/slot"
)

const DEFAULT_PAGE_SIZE = 1024 * 16

const (
	TYPE_INDEX_PAGE byte = iota
	TYPE_DATA_PAGE
)

type Page interface {
	fs.Persistent

	FindRow(key uint32) (Page, int, bool)
	Insert(r interface{}, index int, find bool) uint32
	Len() uint32
	Runtime() PageRuntime
}

//4+1+4+4+8 = 21
type PageHeaderDef struct {
	fs.Persistent
	PageID     *slot.UnsignedInteger //4294967295.0*16/1024/1024/1024 ~= 63.99999998509884 TiB
	Type       *slot.Byte
	Level      *slot.Byte
	Pre        *slot.UnsignedInteger
	Next       *slot.UnsignedInteger
	Checksum   *slot.UnsignedInteger
	LastModify *slot.UnsignedLong    //time.Now().UnixNano()
	ItemSize   *slot.UnsignedInteger //this counter is used to read data from disk
}
type PageTailDef struct {
}

type PageRuntime struct {
	Header     *PageHeaderDef

	pre        Page
	next       Page
	parent     Page

	tree       *PageTree
	byteLength uint32 //finger if the size is larger than 16kb
}

func (r PageRuntime) GetLevel() byte {
	return r.Header.Level.Value
}
func (r PageRuntime) GetItemSize() uint32 {
	return r.Header.ItemSize.Value
}

func (r *PageHeaderDef) ToBytes() []byte {
	ret := r.PageID.ToBytes()
	ret = append(ret, r.Type.ToBytes()...)
	ret = append(ret, r.Level.ToBytes()...)
	ret = append(ret, r.Pre.ToBytes()...)
	ret = append(ret, r.Next.ToBytes()...)
	ret = append(ret, r.Checksum.ToBytes()...)
	ret = append(ret, r.LastModify.ToBytes()...)
	return ret
}

func (r *PageHeaderDef) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	idx += r.PageID.Make(buf, idx + offset)
	idx += r.Type.Make(buf, idx + offset)
	idx += r.Level.Make(buf, idx + offset)
	idx += r.Pre.Make(buf, idx + offset)
	idx += r.Next.Make(buf, idx + offset)
	idx += r.Checksum.Make(buf, idx + offset)
	idx += r.LastModify.Make(buf, idx + offset)
	return idx
}


