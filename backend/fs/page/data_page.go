package page

import (
	"sort"
	"github.com/lycying/pitydb/backend/fs/slot"
)

// DataPage 代表聚类行式存储块，作为最终的索引叶子节点，层级始终为0，其中存储的为多行数据
// Page代表一组统一的块操作，PageRuntime为其代表的数据描述。Content为行内容
type DataPage struct {
	Page

	PageRuntime

	Content []*Row //the tuple data
}

// Runtime 得到运行描述
func (r *DataPage) Runtime() PageRuntime {
	return r.PageRuntime
}

// GetMax 得到页中最小的数字
func (r *DataPage) GetMax() uint32 {
	return r.Content[0].ClusteredKey.Value
}

// Make 通过读取数据块中的数据来填充私有数据
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
	if d.Type.Value == TYPE_INDEX_PAGE {
		_, count, _ := d.FindIndexRow(key)

		count = count - 1
		next := d.tree.mgr.GetPage(d.Content[count].Data[0].(*slot.UnsignedInteger).Value)

		return next.FindRow(key)
	}

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

func (p *DataPage) Insert(obj interface{}, index int, find bool) (Page, uint32) {
	r := obj.(*Row)
	bs := uint32(0)
	bs = p.byteLength + r.Len()
	if find {
		bs = bs - p.Content[index].Len()
		p.Content[index] = r
	} else {
		p.Content = append(p.Content[:index], append([]*Row{r}, p.Content[index:]...)...)
		p.ItemSize.Value++
	}
	p.byteLength = bs

	if bs > DEFAULT_PAGE_SIZE {
		//should split here
		i := 0
		counter := uint32(0)
		for ; i < int(p.Runtime().GetItemSize()); i++ {
			counter = counter + p.Content[i].Len()
			if counter > DEFAULT_PAGE_SIZE {
				break
			}
		}

		newNode := p.tree.NewDataPage(0, p.Type.Value)
		//copy [i-1:] to newNode
		newNode.InsertRows(p.Content[i:])
		//reduce the orig node
		p.PageReduce(0, i)

		if p.parent == nil {
			newRoot := p.tree.NewDataPage(p.Level.Value + 1, TYPE_INDEX_PAGE)

			indexRowForOld := NewIndexRow()
			indexRowForOld.Data[0].(*slot.UnsignedInteger).Value = p.PageID.Value
			indexRowForOld.ClusteredKey.Value = p.GetMax()
			newRoot.Insert(indexRowForOld, 0, false)
			p.parent = newRoot

			indexRowForNew := NewIndexRow()
			indexRowForNew.Data[0].(*slot.UnsignedInteger).Value = newNode.PageID.Value
			indexRowForNew.ClusteredKey.Value = newNode.GetMax()
			newRoot.Insert(indexRowForNew, 1, false)
			newNode.parent = newRoot

			p.tree.root = newRoot
		} else {
			indexRowForNew := NewIndexRow()
			indexRowForNew.Data[0].(*slot.UnsignedInteger).Value = newNode.PageID.Value
			indexRowForNew.ClusteredKey.Value = newNode.GetMax()

			_, toIndex, _ := p.parent.FindIndexRow(indexRowForNew.ClusteredKey.Value)
			myParent, _ := p.parent.Insert(indexRowForNew, toIndex, false)
			newNode.parent = myParent
		}

	}
	return p, bs
}

func (p *DataPage) Delete(key uint32, index int) {
	p.Content = append(p.Content[:index], p.Content[index + 1:]...)
	p.ItemSize.Value--
	p.byteLength = p.Len()
}

func (p *DataPage) InsertRows(rs []*Row) {
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
func (p *DataPage) FindIndexRow(key uint32) (Page, int, bool) {

	count := 0

	size := len(p.Content)
	for i := size - 1; i >= 0; i-- {
		count = i
		if key >= p.Content[i].ClusteredKey.Value {
			break
		}
	}
	return p, count + 1, true


	//val_len := len(p.Content)
	//
	//i := sort.Search(val_len, func(i int) bool {
	//	return key <= p.Content[i].KeyWordMark.Value
	//})
	//
	////the rows is empty
	//if i == 0 && val_len == 0 {
	//	return nil, 0, false
	//}
	//
	////should put at the tail of the row array
	//if i >= val_len {
	//	i = val_len
	//}
	//
	//return p, i, true

}

