package page

import (
	"github.com/lycying/pitydb/backend/fs/row"
)

const DEFAULT_PAGE_SIZE = 1024 * 16

const (
	IndexPage byte = iota
	DataPage
)

//4+1+4+4+8 = 21
type PageHeaderDef struct {
	PageID     uint32 //4294967295.0*16/1024/1024/1024 ~= 63.99999998509884 TiB
	Type       byte
	Pre        uint32
	Next       uint32
	Checksum   uint32
	LastModify uint64 //time.Now().UnixNano()
}
type PageTailDef struct {
}
type PageDef struct {
	PageHeaderDef
	Val []*row.Tuple //the tuple data
	PageTailDef
}

func NewPageDef() {
}