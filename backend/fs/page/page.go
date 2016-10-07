package page

import (
	"github.com/lycying/pitydb/backend/fs"
	"github.com/lycying/pitydb/backend/fs/slot"
	"sort"
	"bytes"
)

const DefaultPageSize = 1024 * 16

const (
	indexPageType byte = iota
	dataPageType
)

type pageHeader struct {
	fs.Persistent
	pgID       *slot.UnsignedInteger //4294967295.0*16/1024/1024/1024 ~= 63.99999998509884 TiB
	pgType     *slot.Byte
	level      *slot.Byte
	left       *slot.UnsignedInteger
	right      *slot.UnsignedInteger
	checksum   *slot.UnsignedInteger
	lastModify *slot.UnsignedLong    //time.Now().UnixNano()
	size       *slot.UnsignedInteger //this counter is used to read data from disk
}

func (header *pageHeader) ToBytes() []byte {

	bPgID := header.pgID.ToBytes()
	bPgType := header.pgType.ToBytes()
	bLevel := header.level.ToBytes()
	bLeft := header.left.ToBytes()
	bRight := header.right.ToBytes()
	bChecksum := header.checksum.ToBytes()
	bLastModify := header.lastModify.ToBytes()

	cap := len(bPgID)
	cap += len(bPgType)
	cap += len(bLevel)
	cap += len(bLeft)
	cap += len(bRight)
	cap += len(bChecksum)
	cap += len(bLastModify)

	buf := bytes.NewBuffer(make([]byte, cap))

	buf.Write(bPgID)
	buf.Write(bPgType)
	buf.Write(bLevel)
	buf.Write(bLeft)
	buf.Write(bRight)
	buf.Write(bChecksum)
	buf.Write(bLastModify)

	return buf.Bytes()
}


func (header *pageHeader) Make(buf []byte, offset uint32) uint32 {

	idx := uint32(0)
	idx += header.pgID.Make(buf, idx + offset)
	idx += header.pgType.Make(buf, idx + offset)
	idx += header.level.Make(buf, idx + offset)
	idx += header.left.Make(buf, idx + offset)
	idx += header.right.Make(buf, idx + offset)
	idx += header.checksum.Make(buf, idx + offset)
	idx += header.lastModify.Make(buf, idx + offset)

	return idx
}

type Page struct {
	pageHeader

	pre      *Page
	next     *Page
	parent   *Page

	tree     *PageTree

	data     []*Row //the tuple data
	_byteLen uint32 //finger if the size is larger than 16kb
}

func (p *Page) getMinKey() uint32 {
	return p.data[0].Key.Value
}

func (p *Page) Make(buf []byte, offset uint32) uint32 {
	idx := uint32(0)
	idx = p.pageHeader.Make(buf, idx + offset)
	for _, v := range p.data {
		idx += v.Make(buf, idx + offset)
	}
	return idx
}

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

func (p *Page) insert(row *Row, index int, find bool) uint32 {
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
				bs = counter - p.data[i].Len()
				break
			}
		}

		newPage := p.tree.NewPage(p.level.Value, p.pgType.Value)
		//copy [:i-1] to newNode
		newPage.copyRightPart(p, i - 1)
		//only left [i-1:] part
		p.deleteRightPart(i - 1)

		if p.hasParent() {
			indexRow := newPage.makeIndexRow()
			_, toIndex, _ := p.parent.findIndexRow(indexRow.Key.Value)
			p.parent.insert(indexRow, toIndex, false)
			newPage.parent = p.parent

		} else {
			newRoot := p.tree.NewIndexPage(p.level.Value + 1)

			indexRow0 := p.makeIndexRow()
			newRoot.insert(indexRow0, 0, false)
			p.parent = newRoot

			indexRow1 := newPage.makeIndexRow()
			newRoot.insert(indexRow1, 1, false)
			newPage.parent = newRoot

			p.tree.root = newRoot
		}

	}
	return bs
}

func (p *Page) delete(key uint32, index int) {
	p.data = append(p.data[:index], p.data[index + 1:]...)
	p.size.Value--
	p._byteLen = p.countByteLength()
}

func (p *Page) countByteLength() uint32 {
	ret := uint32(0)
	for _, v := range p.data {
		ret = ret + v.Len()
	}
	return ret
}

func (p *Page) copyRightPart(from *Page, index int) {
	p.data = append(p.data, from.data[index:]...)
	p.size.Value = uint32(len(p.data))
	p._byteLen = p.countByteLength()
}

func (p *Page) deleteRightPart(index int) {
	p.data = p.data[:index]
	p.size.Value = uint32(len(p.data))
	p._byteLen = p.countByteLength()
}

func (p *Page) shouldSplit() bool {
	return p._byteLen > DefaultPageSize
}

func (p *Page) isIndexPage() bool {
	return p.pgType.Value == indexPageType
}
func (p *Page) isDataPage() bool {
	return p.pgType.Value == dataPageType
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
