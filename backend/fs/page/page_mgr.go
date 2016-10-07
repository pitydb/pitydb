package page

type PageMgr  struct {
	pageMap    map[uint32]*Page
	nextPageId uint32
}

func NewPageMgr() *PageMgr {
	return &PageMgr{
		pageMap:make(map[uint32]*Page),
		nextPageId: uint32(0),
	}
}

func (mgr *PageMgr) AddPage(pg *Page) {
	key := pg.PageID.Value
	mgr.pageMap[key] = pg
}

func (mgr *PageMgr) GetPage(pageId uint32) *Page {
	v, ok := mgr.pageMap[pageId]
	if ok {
		return v
	}else {
		println("FUCK.................",pageId)
	}
	//TODO read it from disk
	return v
}

func (mgr *PageMgr) RemovePage(pageId uint32) {
	mgr.pageMap[pageId] = nil
}

func (mgr *PageMgr) NextPageId() uint32 {
	mgr.nextPageId++
	return mgr.nextPageId
}