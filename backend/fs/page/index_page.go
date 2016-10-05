package page

import (
	"github.com/lycying/pitydb/backend/fs/slot"
	"github.com/lycying/pitydb/backend/fs"
	"sort"
)

type IndexRow struct {
	fs.Persistent

	KeyWordMark *slot.UnsignedInteger
	KeyPageId   *slot.UnsignedInteger
}

func (r *IndexRow) ToBytes() []byte {
	ret := r.KeyWordMark.ToBytes()
	ret = append(ret, r.KeyPageId.ToBytes()...)
	return ret
}

func (r *IndexRow) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	idx += r.KeyWordMark.Make(buf, idx + offset)
	idx += r.KeyPageId.Make(buf, idx + offset)
	return idx
}
func (r *IndexRow) Len() uint32 {
	return uint32(8)
}

type IndexPage struct {
	Page
	PageRuntime

	Content []*IndexRow
}

func NewIndexRow() *IndexRow {
	return &IndexRow{
		KeyWordMark:slot.NewUnsignedInteger(0),
		KeyPageId:slot.NewUnsignedInteger(0),
	}
}

func (r *IndexPage) GetMax() uint32 {
	return r.Content[len(r.Content) - 1].KeyWordMark.Value
}
func (r *IndexPage) Runtime() PageRuntime {
	return r.PageRuntime
}

func (r *IndexPage) Len() uint32 {
	return r.ItemSize.Value * uint32(8)
}

func (p *IndexPage) FindIndexRow(key uint32) (Page, int, bool) {
	val_len := len(p.Content)

	i := sort.Search(val_len, func(i int) bool {
		return int(key) <= int(p.Content[i].KeyWordMark.Value)
	})

	//the rows is empty
	if i == 0 && val_len == 0 {
		return nil, 0, false
	}

	//should put at the tail of the row array
	if i >= val_len {
		i = val_len
	}


	return p, i, true

}

func (p *IndexPage) FindRow(key uint32) (Page, int, bool) {
	_, i, _ := p.FindIndexRow(key)

	if i >= len(p.Content) {
		i = len(p.Content) - 1
	}
	next := p.tree.mgr.GetPage(p.Content[i].KeyPageId.Value)

	return next.FindRow(key)

}
func (p *IndexPage) InsertIndexRows(rs []*IndexRow) {
	p.Content = append(p.Content, rs...)
	p.ItemSize.Value = uint32(len(rs))
	p.byteLength = p.Len()
}
func (p *IndexPage) PageReduce(begin, end int) {
	p.Content = p.Content[begin:end]
	p.ItemSize.Value = uint32(end - begin)
	p.byteLength = p.Len()
}
func (p *IndexPage) Insert(obj interface{}, index int, find bool) uint32 {
	r := obj.(*IndexRow)
	bs := uint32(0)
	bs = p.byteLength + r.Len()

	if index >= int(p.ItemSize.Value) {
		p.Content = append(p.Content, r)
	}else {
		p.Content = append(p.Content[:index], append([]*IndexRow{r}, p.Content[index:]...)...)
	}
	p.ItemSize.Value++

	p.byteLength = bs
	if bs > DEFAULT_PAGE_SIZE / 32 {

		//should split here
		i := 0
		counter := uint32(0)
		for ; i < int(p.Runtime().GetItemSize()); i++ {
			counter = counter + p.Content[i].Len()
			if (counter > DEFAULT_PAGE_SIZE / 32) {
				break
			}
		}

		newNode := p.tree.NewIndexPage(p.Level.Value + 1)
		//copy [i-1:] to newNode
		newNode.InsertIndexRows(p.Content[i:])
		//reduce the orig node
		p.PageReduce(0, i)

		//it's the first time that root is full
		if p.parent == nil {
			newRoot := p.tree.NewIndexPage(p.Level.Value + 1)

			indexRowForOld := NewIndexRow()
			indexRowForOld.KeyPageId = p.PageID
			indexRowForOld.KeyWordMark.Value = p.GetMax()
			newRoot.Insert(indexRowForOld, 0, false)
			p.parent = newRoot

			indexRowForNew := NewIndexRow()
			indexRowForNew.KeyPageId = newNode.PageID
			indexRowForNew.KeyWordMark.Value = newNode.GetMax()
			newRoot.Insert(indexRowForNew, 1, false)
			newNode.parent = newRoot

			p.tree.root = newRoot
		}else {
			indexRowForNew := NewIndexRow()
			indexRowForNew.KeyPageId = newNode.PageID
			indexRowForNew.KeyWordMark.Value = newNode.GetMax()

			_, toIndex, _ := p.parent.(*IndexPage).FindIndexRow(r.KeyWordMark.Value)
			p.parent.Insert(indexRowForNew, toIndex, false)
			newNode.parent = p.parent
		}

		if nil != p.parent {
			print("insert:", r.KeyWordMark.Value, "\t")
			print("L", p.parent.Runtime().Level.Value)
			for _, xxx := range p.parent.(*IndexPage).Content {
				ux := p.tree.mgr.GetPage(xxx.KeyPageId.Value)
				if ux.Runtime().Type.Value == TYPE_INDEX_PAGE {
					vp := ux.(*IndexPage)
					print(" ", vp.PageID.Value)
					print("(")
					for _, px := range vp.Content {
						print(px.KeyWordMark.Value, ",")
					}
					print(")")
				}else if ux.Runtime().Type.Value == TYPE_DATA_PAGE {
					vp := ux.(*DataPage)
					print("(")
					for _, px := range vp.Content {
						print(px.ClusteredKey.Value, ",")
					}
					print(")")
				}
			}
		}
		println()
	}

	return bs
}
