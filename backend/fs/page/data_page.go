package page

import (
	"sort"
	"github.com/lycying/pitydb/backend/fs/row"
)

type DataPage struct {
	Page

	PageRuntime

	Content []*row.Row //the tuple data
}

func (r *DataPage) Runtime() PageRuntime {
	return r.PageRuntime
}

func (r *DataPage) GetMax() uint32 {
	return r.Content[0].ClusteredKey.Value
}

func (r *DataPage) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	idx = r.PageHeader.Make(buf, idx + offset)
	for _, v := range r.Content {
		idx += v.Make(buf, idx + offset)
	}
	return idx
}
func (r *DataPage) ToBytes() []byte {
	ret := make([]byte, 0)
	ret = append(ret, r.PageHeader.ToBytes()...)
	for _, v := range r.Content {
		ret = append(ret, v.ToBytes()...)
	}
	return ret
}

func (d *DataPage) FindRow(key uint32) (Page, int, bool) {
	val_len := int(d.ItemSize.Value)

	i := sort.Search(val_len, func(i int) bool {
		return key <= d.Content[i].ClusteredKey.Value
	})
	//the rows is empty
	if i == 0 && val_len == 0 {
		return d, 0, false
	}

	//should put at the tail of the row array
	if i >= val_len {
		return d, val_len, false
	}

	ckey := d.Content[i].ClusteredKey.Value
	if ckey == key {
		return d, i, true
	}
	return d, i, false
}

func (p *DataPage) Insert(obj interface{}, index int, find bool) (Page,uint32) {
	r := obj.(*row.Row)
	bs := uint32(0)
	bs = p.byteLength + r.Len()
	if find {
		bs = bs - p.Content[index].Len()
		p.Content[index] = r
	}else {
		p.Content = append(p.Content[:index], append([]*row.Row{r}, p.Content[index:]...)...)
		p.ItemSize.Value++
	}
	p.byteLength = bs

	if bs > DEFAULT_PAGE_SIZE {
		//should split here
		i := 0
		counter := uint32(0)
		for ; i < int(p.Runtime().GetItemSize()); i++ {
			counter = counter + p.Content[i].Len()
			if (counter > DEFAULT_PAGE_SIZE) {
				break
			}
		}

		newNode := p.tree.NewDataPage(0)
		//copy [i-1:] to newNode
		newNode.InsertRows(p.Content[i:])
		//reduce the orig node
		p.PageReduce(0, i)

		//for _, xxx := range p.Content {
		//	print(xxx.ClusteredKey.Value, ",")
		//}
		//print("#")
		//for _, xxx := range newNode.Content {
		//	print(xxx.ClusteredKey.Value, ",")
		//}
		//println("|", r.ClusteredKey.Value)
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

			_, toIndex, _ := p.parent.(*IndexPage).FindIndexRow(indexRowForNew.KeyWordMark.Value)
			myParent, _ := p.parent.Insert(indexRowForNew, toIndex, false)
			newNode.parent = myParent
		}

	}
	p.tree.Dump()

	return p,bs
}

func (p *DataPage) Delete(key uint32, index int) {
	p.Content = append(p.Content[:index], p.Content[index + 1:]...)
	p.ItemSize.Value--
	p.byteLength = p.Len()
}

func (p *DataPage) InsertRows(rs []*row.Row) {
	p.Content = append(p.Content, rs...)
	p.ItemSize.Value = uint32(len(rs))
	p.byteLength = p.Len()
}

func (p *DataPage) Len() uint32 {
	ret := uint32(0)
	for _, v := range p.Content {
		ret = ret + v.Len()
	}
	return ret
}

func (p *DataPage) PageReduce(begin, end int) {
	p.Content = p.Content[begin:end]
	p.ItemSize.Value = uint32(end - begin)
	p.byteLength = p.Len()
}
