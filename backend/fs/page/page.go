package page

import (
	"github.com/lycying/pitydb/backend/fs/row"
	"github.com/lycying/pitydb/backend/fs"
	"github.com/lycying/pitydb/backend/fs/slot"
	"os"
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
	Header      *PageHeaderDef

	ItemSize    *slot.UnsignedInteger
	Pre         *Page
	Next        *Page
	Parent      *Page

	Root        *PageTree
	CurrentSize uint32 //finger if the size is larger than 16kb

	Context     PageContext
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
	r.CurrentSize = idx
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
func (d *DataPage) FindRow(key uint32) *row.Row {
	return nil
}
func (d *DataPage) Insert(r *row.Row) {

}
func (p *IndexPage) FindPage(key uint32) *Page {
	return nil
}

func (tree *PageTree) FindRow(key uint32) *row.Row {
	root := tree.Root
	if root.Header.Level.Val == 0 {
		return root.Context.(*DataPage).FindRow(key)
	}
	return root.FindRowLoop(key)
}
func (tree *PageTree) FindDataPage(key uint32) *Page {
	root := tree.Root
	return root.FindPageLoop(key)
}

func (tree *PageTree) InsertOrUpdate(r *row.Row) {
	key := r.ClusteredKey.Val
	node := tree.FindDataPage(key)
	print(node)
	data := node.Context.(*DataPage)
	data.Val = append(data.Val, r)
}
func (p *Page) FindPageLoop(key uint32) *Page {
	if p.Header.Type.Val == TYPE_DATA_PAGE {
		return p
	}
	tmp := p.Context.(*IndexPage).FindPage(key)
	return tmp
}
func (p *Page) FindRowLoop(key uint32) *row.Row {
	if p.Header.Type.Val == TYPE_DATA_PAGE {
		return p.Context.(*DataPage).FindRow(key)
	}
	tmp := p.Context.(*IndexPage).FindPage(key)
	return tmp.FindRowLoop(key)
}
