package page

import (
	"os"
	"github.com/lycying/pitydb/backend/fs/slot"
)

type PageTree struct {
	root *Page
	meta *RowMeta
	link *os.File
	mgr  *PageMgr
}

func NewPageTree(meta *RowMeta, link *os.File) *PageTree {
	mgr := NewPageMgr()
	root := &Page{
		pageHeader:pageHeader{
			pgID:slot.NewUnsignedInteger(mgr.NextPageId()),
			typ: slot.NewByte(DataPageType),
			level:slot.NewByte(0x00),
			left:slot.NewUnsignedInteger(0),
			right:slot.NewUnsignedInteger(0),
			checksum:slot.NewUnsignedInteger(0),
			lastModify:slot.NewUnsignedLong(0),
			size:slot.NewUnsignedInteger(0),
		},
		_byteLen:uint32(0),
		parent:nil,
		data:[]*Row{},
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
func (tree *PageTree) NewIndexPage(level byte) *Page {
	return tree.NewPage(level, IndexPageType)
}
func (tree *PageTree) NewDataPage(level byte) *Page {
	return tree.NewPage(level, DataPageType)
}
func (tree *PageTree) NewPage(level byte, t byte) *Page {
	p := &Page{
		pageHeader:pageHeader{
			pgID:slot.NewUnsignedInteger(tree.mgr.NextPageId()),
			typ:slot.NewByte(t),
			level:slot.NewByte(level),
			left:slot.NewUnsignedInteger(0),
			right:slot.NewUnsignedInteger(0),
			checksum:slot.NewUnsignedInteger(0),
			lastModify:slot.NewUnsignedLong(0),
			size:slot.NewUnsignedInteger(0),
		},
		tree:tree,
		_byteLen:0,
		data:[]*Row{},
	}
	tree.mgr.AddPage(p)
	return p
}

func (tree *PageTree) Insert(r *Row) {
	key := r.Key.Value

	node, idx, find := tree.FindOne(key)

	//the row is so big that one default can not hold it
	if r.Len() > DefaultPageSize {
		//TODO big row storage
	}
	node.insert(r, idx, find)

}
func (tree *PageTree) Delete(key uint32) bool {
	node, idx, find := tree.FindOne(key)
	if find {
		node.delete(key, idx)
		return true
	}
	return false
}
func (tree *PageTree) FindOne(key uint32) (*Page, int, bool) {
	return tree.root.findOne(key)
}

func (tree *PageTree) Dump(level int) {
	println("BEGIN")
	root := tree.root
	dumpPage(root)
	println("END")
	println("")
}
func _getParentPageID(pg *Page) uint32 {
	if pg.parent == nil {
		return 0
	}else {
		return pg.parent.pgID.Value
	}
}
func dumpPage(pg *Page) {
	if nil == pg {
		return
	}
	if pg.typ.Value == DataPageType {
		print(pg.level.Value, "D`", pg.pgID.Value, "@", _getParentPageID(pg), "`\t:(")
		for _, x := range pg.data {
			print(x.Key.Value, ",")
		}
		print(")")
		println()
	}else {
		print(pg.level.Value, "I`", pg.pgID.Value, "@", _getParentPageID(pg), "`\t:[")
		for _, x := range pg.data {
			print(x.Key.Value, ",")
		}
		print("]")
		println()
		for _, x := range pg.data {
			px := pg.tree.mgr.GetPage(x.Data[0].(*slot.UnsignedInteger).Value)
			dumpPage(px)
		}
	}
}