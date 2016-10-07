package page

import (
	"github.com/lycying/pitydb/backend/fs"
	"github.com/lycying/pitydb/backend/fs/slot"
	"sort"
)

const DefaultPageSize = 1024 * 16

const (
	IndexPageType byte = iota
	DataPageType
)

type pageHeader struct {
	fs.Persistent
	pgID       *slot.UnsignedInteger //4294967295.0*16/1024/1024/1024 ~= 63.99999998509884 TiB
	typ        *slot.Byte
	level      *slot.Byte
	left       *slot.UnsignedInteger
	right      *slot.UnsignedInteger
	checksum   *slot.UnsignedInteger
	lastModify *slot.UnsignedLong    //time.Now().UnixNano()
	size       *slot.UnsignedInteger //this counter is used to read data from disk
}

func (header *pageHeader) ToBytes() []byte {
	ret := header.pgID.ToBytes()
	ret = append(ret, header.typ.ToBytes()...)
	ret = append(ret, header.level.ToBytes()...)
	ret = append(ret, header.left.ToBytes()...)
	ret = append(ret, header.right.ToBytes()...)
	ret = append(ret, header.checksum.ToBytes()...)
	ret = append(ret, header.lastModify.ToBytes()...)
	return ret
}

func (header *pageHeader) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	idx += header.pgID.Make(buf, idx + offset)
	idx += header.typ.Make(buf, idx + offset)
	idx += header.level.Make(buf, idx + offset)
	idx += header.left.Make(buf, idx + offset)
	idx += header.right.Make(buf, idx + offset)
	idx += header.checksum.Make(buf, idx + offset)
	idx += header.lastModify.Make(buf, idx + offset)
	return idx
}

// DataPage 代表聚类行式存储块，作为最终的索引叶子节点，层级始终为0，其中存储的为多行数据
// Page代表一组统一的块操作，PageRuntime为其代表的数据描述。Content为行内容
type Page struct {
	pageHeader

	pre      *Page
	next     *Page
	parent   *Page

	tree     *PageTree

	data     []*Row //the tuple data
	_byteLen uint32 //finger if the size is larger than 16kb
}

// GetMax 得到页中最小的数字
func (p *Page) getMinKey() uint32 {
	return p.data[0].Key.Value
}

// Make 通过读取数据块中的数据来填充私有数据
func (p *Page) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	idx = p.pageHeader.Make(buf, idx + offset)
	for _, v := range p.data {
		idx += v.Make(buf, idx + offset)
	}
	return idx
}
// ToBytes 生成字节
func (p *Page) ToBytes() []byte {
	ret := make([]byte, 0)
	ret = append(ret, p.pageHeader.ToBytes()...)
	for _, v := range p.data {
		ret = append(ret, v.ToBytes()...)
	}
	return ret
}
func (p *Page) findIndexRow(key uint32) (*Page, int, bool) {
	count := 0

	size := len(p.data)
	for i := size - 1; i >= 0; i-- {
		count = i
		if key >= p.data[i].Key.Value {
			break
		}
	}
	return p, count + 1, true
}

func (p *Page) findOne(key uint32) (*Page, int, bool) {
	if p.isIndexPage() {
		_, count, _ := p.findIndexRow(key)

		count = count - 1
		next := p.tree.mgr.GetPage(p.data[count].Data[0].(*slot.UnsignedInteger).Value)

		return next.findOne(key)
	}

	val_len := int(p.size.Value)

	i := sort.Search(val_len, func(i int) bool {
		return key <= p.data[i].Key.Value
	})
	//the rows is empty
	if i == 0 && val_len == 0 {
		return p, 0, false
	}

	//should put at the tail of the row array
	if i >= val_len {
		return p, val_len, false
	}

	if p.data[i].Key.Value == key {
		return p, i, true
	}
	return p, i, false
}

func (p *Page) insert(row *Row, index int, find bool) (*Page, uint32) {
	bs := uint32(0)
	bs = p._byteLen + row.Len()
	if find {
		bs = bs - p.data[index].Len()
		p.data[index] = row
	} else {
		p.data = append(p.data[:index], append([]*Row{row}, p.data[index:]...)...)
		p.size.Value++
	}
	p._byteLen = bs

	if p.shouldSplit() {
		//should split here
		i := 0
		counter := uint32(0)
		for ; i < int(p.size.Value); i++ {
			counter = counter + p.data[i].Len()
			if counter > DefaultPageSize {
				break
			}
		}

		newNode := p.tree.NewPage(0, p.typ.Value)
		//copy [i-1:] to newNode
		newNode.copyToLeftPart(p.data[i:])
		//reduce the orig node
		p.deleteRightPart(i)

		if p.hasParent() {
			indexRowForNew := newNode.makeIndexRow()
			_, toIndex, _ := p.parent.findIndexRow(indexRowForNew.Key.Value)
			myParent, _ := p.parent.insert(indexRowForNew, toIndex, false)
			newNode.parent = myParent

		} else {
			newRoot := p.tree.NewIndexPage(p.level.Value + 1)

			indexRowForOld := p.makeIndexRow()
			newRoot.insert(indexRowForOld, 0, false)
			p.parent = newRoot

			indexRowForNew := newNode.makeIndexRow()
			newRoot.insert(indexRowForNew, 1, false)
			newNode.parent = newRoot

			p.tree.root = newRoot
		}

	}
	return p, bs
}

func (p *Page) delete(key uint32, index int) {
	p.data = append(p.data[:index], p.data[index + 1:]...)
	p.size.Value--
	p._byteLen = p.len()
}

func (p *Page) len() uint32 {
	ret := uint32(0)
	for _, v := range p.data {
		ret = ret + v.Len()
	}
	return ret
}

func (p *Page) copyToLeftPart(rs []*Row) {
	p.data = append(p.data, rs...)
	p.size.Value = uint32(len(rs))
	p._byteLen = p.len()
}

func (p *Page) deleteRightPart(index int) {
	p.data = p.data[:index]
	p.size.Value = uint32(index)
	p._byteLen = p.len()
}

func (p *Page) shouldSplit() bool {
	return p._byteLen > DefaultPageSize
}

func (p *Page) isIndexPage() bool {
	return p.typ.Value == IndexPageType
}
func (p *Page) isDataPage() bool {
	return p.typ.Value == DataPageType
}

func (p *Page) hasParent() bool {
	return p.parent != nil
}

func (p *Page) makeIndexRow() *Row {
	meta := &RowMeta{
		Type:slot.ST_UNSIGNED_INTEGER,
	}
	r := NewRow(meta)
	r.Data = append(r.Data, slot.NewUnsignedInteger(p.pgID.Value))
	r.Key.Value = p.getMinKey()
	return r
}
