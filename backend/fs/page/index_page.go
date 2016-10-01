package page

import "github.com/lycying/pitydb/backend/fs/slot"

type IndexRow struct {
	KeyWordMark *slot.UnsignedInteger
	KeyPageId   *slot.UnsignedInteger
}

type IndexPage struct {
	Page
	PageRuntime

	Content []*IndexRow
}

func (r *IndexPage) Runtime() PageRuntime {
	return r.PageRuntime
}

func (p *IndexPage) FindPage(key uint32) *PageRuntime {
	return nil
}
func (p *IndexPage) FindRow(key uint32) (Page, int, bool) {
	return nil, 0, true
}
func (p *IndexPage) Insert(obj interface{}, index int, find bool) uint32 {
	return uint32(0)
}
