package page

import (
	"os"
	"github.com/lycying/pitydb/backend/fs/slot"
)

type PageTree struct {
	root Page
	meta *RowMeta
	link *os.File
	mgr  *PageMgr
}

func NewPageTree(meta *RowMeta, link *os.File) *PageTree {
	mgr := NewPageMgr()
	root := &DataPage{
		PageRuntime: PageRuntime{
			PageHeader:PageHeader{
				PageID:slot.NewUnsignedInteger(mgr.NextPageId()),
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
		Content:[]*Row{},
	}

	tree := &PageTree{
		meta:meta,
		link:link,
		root:root,
		mgr:mgr,
	}
	root.tree = tree

	tree.mgr.AddPage(root)
	return tree

}
func (tree *PageTree) NewDataPage(level byte) *DataPage {
	p := &DataPage{
		PageRuntime:PageRuntime{
			PageHeader:PageHeader{
				PageID:slot.NewUnsignedInteger(tree.mgr.NextPageId()),
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
		Content:[]*Row{},
	}
	tree.mgr.AddPage(p)
	return p
}
func (tree *PageTree) NewIndexPage(level byte) *IndexPage {
	p := &IndexPage{
		PageRuntime:PageRuntime{
			PageHeader:PageHeader{
				PageID:slot.NewUnsignedInteger(tree.mgr.NextPageId()),
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
	tree.mgr.AddPage(p)
	return p
}

func (tree *PageTree) InsertRow(r *Row) {
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

func (tree *PageTree) Dump(level int) {
	println("BEGIN")
	root := tree.root
	dumpPage(root, level)
	println("END")
	println("")
}
func _getParent(pg Page) uint32 {
	if pg.Runtime().parent == nil {
		return 0
	}else {
		return pg.Runtime().parent.Runtime().PageID.Value
	}
}
func dumpPage(pg Page, level int) {
	if nil == pg {
		return
	}
	if pg.Runtime().Type.Value == TYPE_DATA_PAGE {
		if level > 0 {
			v := pg.(*DataPage)
			print(v.Level.Value, "D`", v.PageID.Value, "@", _getParent(v), "`\t:(")
			for _, x := range v.Content {
				print(x.ClusteredKey.Value, ",")
			}
			print(")")
			println()
		}
	}else {
		v := pg.(*IndexPage)
		print(v.Level.Value, "I`", v.PageID.Value, "@", _getParent(v), "`\t:[")
		for _, x := range v.Content {
			print(x.KeyWordMark.Value, ",")
		}
		print("]")
		println()
		for _, x := range v.Content {
			px := v.tree.mgr.GetPage(x.KeyPageId.Value)
			dumpPage(px,level)
		}
	}
}