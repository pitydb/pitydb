package page

import (
	"os"
	"github.com/lycying/pitydb/backend/fs/row"
	"github.com/lycying/pitydb/backend/fs/slot"
)

type PageTree struct {
	root Page
	meta *row.RowMeta
	link *os.File
}

func NewPageTree(meta *row.RowMeta, link *os.File) *PageTree {
	root := &DataPage{
		PageRuntime: PageRuntime{
			PageHeader:PageHeader{
				PageID:slot.NewUnsignedInteger(0),
				Type:slot.NewByte(TYPE_DATA_PAGE),
				Level:slot.NewByte(0x00),
				Pre:slot.NewUnsignedInteger(0),
				Next:slot.NewUnsignedInteger(0),
				Checksum:slot.NewUnsignedInteger(0),
				LastModify:slot.NewUnsignedLong(0),
				ItemSize:slot.NewUnsignedInteger(0),
			},
			byteLength:uint32(0),
			parent:nil,
		},
		Content:[]*row.Row{},
	}

	return &PageTree{
		meta:meta,
		link:link,
		root:root,
	}

}
func (tree *PageTree) NewDataPage(level byte) *DataPage {
	p := &DataPage{
		PageRuntime:PageRuntime{
			PageHeader:PageHeader{
				PageID:slot.NewUnsignedInteger(1), //TODO next id
				Type:slot.NewByte(TYPE_DATA_PAGE),
				Level:slot.NewByte(level),
				Pre:slot.NewUnsignedInteger(0),
				Next:slot.NewUnsignedInteger(0),
				Checksum:slot.NewUnsignedInteger(0),
				LastModify:slot.NewUnsignedLong(0),
				ItemSize:slot.NewUnsignedInteger(0),
			},
			tree:tree,
			byteLength:0,
		},
		Content:[]*row.Row{},
	}
	return p
}
func (tree *PageTree) NewIndexPage(level byte) *IndexPage {
	p := &IndexPage{
		PageRuntime:PageRuntime{
			PageHeader:PageHeader{
				PageID:slot.NewUnsignedInteger(1), //TODO next id
				Type:slot.NewByte(TYPE_INDEX_PAGE),
				Level:slot.NewByte(level),
				Pre:slot.NewUnsignedInteger(0),
				Next:slot.NewUnsignedInteger(0),
				Checksum:slot.NewUnsignedInteger(0),
				LastModify:slot.NewUnsignedLong(0),
				ItemSize:slot.NewUnsignedInteger(0),
			},
			tree:tree,
			byteLength:0,
		},
		Content:[]*IndexRow{},
	}
	return p
}

func (tree *PageTree) InsertRow(r *row.Row) {
	key := r.ClusteredKey.Value

	node, idx, find := tree.FindRow(key)

	//the row is so big that one default can not hold it
	if r.Len() > DEFAULT_PAGE_SIZE {
		//TODO big row storage
	}
	node.Insert(r, idx, find)

}
func (tree *PageTree) Delete(key uint32) bool {
	node, idx, find := tree.FindRow(key)
	if find {
		node.(*DataPage).Delete(key, idx)
		return true
	}
	return false
}
func (tree *PageTree) FindRow(key uint32) (Page, int, bool) {
	return tree.root.FindRow(key)
}
