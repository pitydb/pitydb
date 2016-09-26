package page

import (
	"testing"
	"github.com/lycying/pitydb/backend/fs/slot"
	"github.com/lycying/pitydb/backend/fs/row"
	"os"
)

func TestNewPage(t *testing.T) {
	meta := &row.RowMeta{
		Type:slot.ST_ROOT,
		Children:[]*row.RowMeta{
			&row.RowMeta{Type:slot.ST_STRING},
			&row.RowMeta{Type:slot.ST_INTEGER},
			&row.RowMeta{Type:slot.ST_STRING},
			&row.RowMeta{Type:slot.ST_DOUBLE},
			&row.RowMeta{Type:slot.ST_BOOL},
			&row.RowMeta{Type:slot.ST_STRING},
		},
	}
	link, _ := os.OpenFile("/tmp/b", os.O_RDWR, 0666)
	tree := NewPageTree(meta, link)

	tree.Root = &Page{
		Header:&PageHeaderDef{
			PageID:slot.NewUnsignedInteger(0),
			Type:slot.NewByte(TYPE_DATA_PAGE),
			Level:slot.NewByte(0x00),
			Pre:slot.NewUnsignedInteger(0),
			Next:slot.NewUnsignedInteger(0),
			Checksum:slot.NewUnsignedInteger(0),
			LastModify:slot.NewUnsignedLong(0),
		},
		ItemSize:slot.NewUnsignedInteger(0),
	}
	tree.Root.Context = &DataPage{
		Holder:tree.Root,

	}

	for i := 1; i < 20; i++ {
		r := row.NewRow(meta)
		r.Fill(
			slot.NewString("skflksfsfdsjflsjfslfj"),
			slot.NewInteger(int32(100)),
			slot.NewString("内地"),
			slot.NewDouble(float64(0.234242423423)),
			slot.NewBoolean(true),
			slot.NewString("卡卡老师封疆大吏舒服的沙发"),
		)
		r.ClusteredKey = slot.NewUnsignedInteger(uint32(i * 2))

		tree.InsertOrUpdate(r)
	}
	for i := 1; i <= 20; i++ {
		r := row.NewRow(meta)
		r.Fill(
			slot.NewString("skflksfsfdsjflsjfslfj"),
			slot.NewInteger(int32(100)),
			slot.NewString("内地"),
			slot.NewDouble(float64(0.234242423423)),
			slot.NewBoolean(true),
			slot.NewString("卡卡老师封疆大吏舒服的沙发"),
		)
		r.ClusteredKey = slot.NewUnsignedInteger(uint32(i * 2))

		tree.InsertOrUpdate(r)
	}
	for i := 20; i > 0; i-- {
		r := row.NewRow(meta)
		r.Fill(
			slot.NewString("skflksfsfdsjflsjfslfj"),
			slot.NewInteger(int32(100)),
			slot.NewString("内地"),
			slot.NewDouble(float64(0.234242423423)),
			slot.NewBoolean(true),
			slot.NewString("卡卡老师封疆大吏舒服的沙发"),
		)
		r.ClusteredKey = slot.NewUnsignedInteger(uint32(i * 2 + 1))

		tree.InsertOrUpdate(r)
	}

}
