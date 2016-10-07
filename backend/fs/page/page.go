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

//4+1+4+4+8 = 21
type PageHeader struct {
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


func (r *PageHeader) ToBytes() []byte {
	ret := r.PageID.ToBytes()
	ret = append(ret, r.Type.ToBytes()...)
	ret = append(ret, r.Level.ToBytes()...)
	ret = append(ret, r.Pre.ToBytes()...)
	ret = append(ret, r.Next.ToBytes()...)
	ret = append(ret, r.Checksum.ToBytes()...)
	ret = append(ret, r.LastModify.ToBytes()...)
	return ret
}

func (r *PageHeader) Make(buf []byte, offset uint32) uint32 {
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

