package page

import (
	"github.com/lycying/pitydb/backend/fs/row"
	"github.com/lycying/pitydb/backend/fs"
	"github.com/lycying/pitydb/backend/fs/slot"
	"os"
	"sort"
)

const DEFAULT_PAGE_SIZE = 1024 * 16

const (
	TYPE_INDEX_PAGE byte = iota
	TYPE_DATA_PAGE
)

type PageContext interface {
	fs.Persistent
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
}
type PageTailDef struct {
}

type Page struct {
	fs.Persistent
	Header     *PageHeaderDef

	ItemSize   *slot.UnsignedInteger
	Pre        *Page
	Next       *Page
	Parent     *Page

	Root       *PageTree
	ByteLength uint32 //finger if the size is larger than 16kb

	Context    PageContext
}

type IndexPageItem struct {
	KeyWordMark *slot.UnsignedInteger
	KeyPageId   *slot.UnsignedInteger
}

type IndexPage struct {
	PageContext
	Holder *Page

	Data   []*IndexPageItem
}

type DataPage struct {
	PageContext
	Holder *Page

	Val    []*row.Row //the tuple data
}

type PageTree struct {
	Root *Page
	Meta *row.RowMeta
	Link *os.File
}

func NewPageTree(meta *row.RowMeta, link *os.File) *PageTree {
	return &PageTree{
		Meta:meta,
		Link:link,
	}
}
func (r *Page) ToBytes() []byte {
	ret := r.Header.ToBytes()
	ret = append(ret, r.Context.ToBytes()...)
	return ret
}
func (r *DataPage) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	for _, v := range r.Val {
		idx += v.Make(buf, idx + offset)
	}
	return idx
}

func (r *DataPage) ToBytes() []byte {
	ret := make([]byte, 0)
	for _, v := range
	r.Val {
		ret = append(ret, v.ToBytes()...)
	}
	return ret
}

func (r *Page) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	idx += r.Header.Make(buf, idx + offset)
	r.ByteLength = idx
	switch r.Header.Type.Val {
	case TYPE_INDEX_PAGE:
		r.Context = &IndexPage{}
	case TYPE_DATA_PAGE:
		r.Context = &DataPage{}
	}
	return idx
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

func (p *Page) ReadPre() *Page {
	return p.Pre
}
func (p *Page) ReadNext() *Page {
	return p.Pre
}

func (d *DataPage) Insert(r *row.Row) {

}
func (p *IndexPage) FindPage(key uint32) *Page {
	return nil
}

func (tree *PageTree) FindRow(key uint32) (*Page, int, bool) {
	root := tree.Root
	if root.Header.Level.Val == 0 {
		return root.Context.(*DataPage).FindRow(key)
	}
	return root.FindRowLoop(key)
}

func (p *Page) FindRowLoop(key uint32) (*Page, int, bool) {
	if p.Header.Type.Val == TYPE_DATA_PAGE {
		return p.Context.(*DataPage).FindRow(key)
	}
	tmp := p.Context.(*IndexPage).FindPage(key)
	return tmp.FindRowLoop(key)
}
func (d *DataPage) FindRow(key uint32) (*Page, int, bool) {
	val_len := int(d.Holder.ItemSize.Val)

	i := sort.Search(val_len, func(i int) bool {
		return int(key) <= int(d.Val[i].ClusteredKey.Val)
	})
	println("val_len=", val_len, "idx=", i, "key=", key)

	//the rows is empty
	if i == 0 && val_len == 0 {
		return d.Holder, 0, false
	}

	//should put at the tail of the row array
	if i >= val_len {
		return d.Holder, val_len, false
	}

	ckey := d.Val[i].ClusteredKey.Val
	if ckey == key {
		return d.Holder, i, true
	}
	return d.Holder, i, false
}
func (tree *PageTree) InsertOrUpdate(r *row.Row) {
	key := r.ClusteredKey.Val

	node, idx, find := tree.FindRow(key)

	data := node.Context.(*DataPage)
	data.Holder = node

	if find {
		data.Val[idx] = r
	}else {
		data.Val = append(data.Val[:idx], append([]*row.Row{r}, data.Val[idx:]...)...)
		node.ItemSize.Val++
	}
	println("result:", find)
	for _, r := range data.Val {
		print(r.ClusteredKey.Val, " ")
	}
	println("")
	println("")

}

