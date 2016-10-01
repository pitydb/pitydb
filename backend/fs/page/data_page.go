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

func (r *DataPage) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	idx = r.Header.Make(buf, idx + offset)
	for _, v := range r.Content {
		idx += v.Make(buf, idx + offset)
	}
	return idx
}
func (r *DataPage) ToBytes() []byte {
	ret := make([]byte, 0)
	ret = append(ret, r.Header.ToBytes()...)
	for _, v := range r.Content {
		ret = append(ret, v.ToBytes()...)
	}
	return ret
}
func (d *DataPage) FindRow(key uint32) (Page, int, bool) {
	val_len := int(d.Header.ItemSize.Value)

	i := sort.Search(val_len, func(i int) bool {
		return int(key) <= int(d.Content[i].ClusteredKey.Value)
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
func (p *DataPage) Insert(obj interface{}, index int, find bool) uint32 {
	r := obj.(*row.Row)
	bs := uint32(0)
	bs = p.byteLength + r.Len()
	if find {
		bs = bs - p.Content[index].Len()
		p.Content[index] = r
	}else {
		p.Content = append(p.Content[:index], append([]*row.Row{r}, p.Content[index:]...)...)
		p.Header.ItemSize.Value++
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
		newNode.InsertRows(p.Content[0:i - 1])
		//reduce the orig node
		println(i)
		p.PageReduce(0, i - 1)

		println("split...............")
	}

	return bs
}

func (p *DataPage) Delete(key uint32, index int) {
	p.Content = append(p.Content[:index], p.Content[index + 1:]...)
	p.Header.ItemSize.Value--
	p.byteLength = p.Len()
}

func (p *DataPage) InsertRows(rs []*row.Row) {
	p.Content = append(p.Content, rs...)
	p.Header.ItemSize.Value = uint32(len(rs))
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
	if p.Header.Type.Value == TYPE_DATA_PAGE {
		p.Content = p.Content[begin:end]
		p.Header.ItemSize.Value = uint32(end - begin)
		p.byteLength = p.Len()
	}
}
