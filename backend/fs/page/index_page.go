package page
//
//import (
//	"github.com/lycying/pitydb/backend/fs/slot"
//	"github.com/lycying/pitydb/backend/fs"
//)
//
//type IndexRow struct {
//	fs.Persistent
//
//	KeyWordMark *slot.UnsignedInteger
//	KeyPageId   *slot.UnsignedInteger
//}
//
//func (r *IndexRow) ToBytes() []byte {
//	ret := r.KeyWordMark.ToBytes()
//	ret = append(ret, r.KeyPageId.ToBytes()...)
//	return ret
//}
//
//func (r *IndexRow) Make(buf []byte, offset uint32) uint32 {
//	idx := uint32(0)
//	idx += r.KeyWordMark.Make(buf, idx + offset)
//	idx += r.KeyPageId.Make(buf, idx + offset)
//	return idx
//}
//func (r *IndexRow) Len() uint32 {
//	return uint32(8)
//}
//
//type IndexPage struct {
//	Page
//	PageRuntime
//
//	Content []*IndexRow
//}
//
//func NewIndexRow() *IndexRow {
//	return &IndexRow{
//		KeyWordMark:slot.NewUnsignedInteger(0),
//		KeyPageId:slot.NewUnsignedInteger(0),
//	}
//}
//
//func (r *IndexPage) GetMax() uint32 {
//	return r.Content[0].KeyWordMark.Value
//}
//func (r *IndexPage) Runtime() PageRuntime {
//	return r.PageRuntime
//}
//
//func (r *IndexPage) Len() uint32 {
//	return r.ItemSize.Value * uint32(8)
//}
//
//func (p *IndexPage) FindIndexRow(key uint32) (Page, int, bool) {
//
//	count := 0
//
//	size := len(p.Content)
//	for i := size - 1; i >= 0; i-- {
//		count = i
//		if key >= p.Content[i].KeyWordMark.Value {
//			break
//		}
//	}
//	return p, count + 1, true
//
//
//	//val_len := len(p.Content)
//	//
//	//i := sort.Search(val_len, func(i int) bool {
//	//	return key <= p.Content[i].KeyWordMark.Value
//	//})
//	//
//	////the rows is empty
//	//if i == 0 && val_len == 0 {
//	//	return nil, 0, false
//	//}
//	//
//	////should put at the tail of the row array
//	//if i >= val_len {
//	//	i = val_len
//	//}
//	//
//	//return p, i, true
//
//}
//
//func (p *IndexPage) FindRow(key uint32) (Page, int, bool) {
//	_, count, _ := p.FindIndexRow(key)
//
//	count = count - 1
//	next := p.tree.mgr.GetPage(p.Content[count].KeyPageId.Value)
//
//	return next.FindRow(key)
//
//}
//func (p *IndexPage) InsertIndexRows(rs []*IndexRow) {
//	p.Content = append(p.Content, rs...)
//	p.ItemSize.Value = uint32(len(rs))
//	p.byteLength = p.Len()
//}
//func (p *IndexPage) PageReduce(begin, end int) {
//	p.Content = p.Content[begin:end]
//	p.ItemSize.Value = uint32(end - begin)
//	p.byteLength = p.Len()
//}
//func (p *IndexPage) Insert(obj interface{}, index int, find bool) (Page, uint32) {
//	r := obj.(*IndexRow)
//	bs := uint32(0)
//	bs = p.byteLength + r.Len()
//
//	p.Content = append(p.Content[:index], append([]*IndexRow{r}, p.Content[index:]...)...)
//	p.ItemSize.Value++
//	p.byteLength = bs
//
//	if bs > DEFAULT_PAGE_SIZE {
//
//		//should split here
//		i := 0
//		counter := uint32(0)
//		for ; i < int(p.Runtime().GetItemSize()); i++ {
//			counter = counter + p.Content[i].Len()
//			if (counter > DEFAULT_PAGE_SIZE ) {
//				break
//			}
//		}
//
//		newNode := p.tree.NewIndexPage(p.Level.Value + 1)
//		//copy [i-1:] to newNode
//		newNode.InsertIndexRows(p.Content[i:])
//		//reduce the orig node
//		p.PageReduce(0, i)
//
//		//it's the first time that root is full
//		if p.parent == nil {
//			newRoot := p.tree.NewIndexPage(p.Level.Value + 1)
//
//			indexRowForOld := NewIndexRow()
//			indexRowForOld.KeyPageId = p.PageID
//			indexRowForOld.KeyWordMark.Value = p.GetMax()
//			newRoot.Insert(indexRowForOld, 0, false)
//			p.parent = newRoot
//
//			indexRowForNew := NewIndexRow()
//			indexRowForNew.KeyPageId = newNode.PageID
//			indexRowForNew.KeyWordMark.Value = newNode.GetMax()
//			newRoot.Insert(indexRowForNew, 1, false)
//			newNode.parent = newRoot
//
//			p.tree.root = newRoot
//		}else {
//			indexRowForNew := NewIndexRow()
//			indexRowForNew.KeyPageId = newNode.PageID
//			indexRowForNew.KeyWordMark.Value = newNode.GetMax()
//
//			_, toIndex, _ := p.parent.(*IndexPage).FindIndexRow(indexRowForNew.KeyWordMark.Value)
//			myParent, _ := p.parent.Insert(indexRowForNew, toIndex, false)
//			newNode.parent = myParent
//			return newNode, bs
//		}
//	}
//	return p, bs
//}
