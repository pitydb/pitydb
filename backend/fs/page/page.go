package page

import "github.com/lycying/laladb/backend/fs/tuple"

const DEFAULT_PAGE_SIZE = 1024 * 16

type DataPage struct {
	PageID uint32         //4294967295.0*16/1024/1024/1024 ~= 63.99999998509884 TiB
	Val    []*tuple.Tuple //the tuple data
}

type IndexPage struct {
}
