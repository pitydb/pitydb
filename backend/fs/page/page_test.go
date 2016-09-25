package page

import (
	"testing"
	"github.com/lycying/pitydb/backend/fs/slot"
	"github.com/lycying/pitydb/backend/fs/row"
	"os"
	"fmt"
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

	r := row.NewRow(meta)
	r.Fill(
		slot.NewString("skflksfsfdsjflsjfslfj"),
		slot.NewInteger(int32(100)),
		slot.NewString("内地"),
		slot.NewDouble(float64(0.234242423423)),
		slot.NewBoolean(true),
		slot.NewString("卡卡老师封疆大吏舒服的沙发"),
	)
	r.ClusteredKey = slot.NewUnsignedInteger(uint32(1))
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
	}
	tree.Root.Context = &DataPage{
	}
	tree.InsertOrUpdate(r)
	fmt.Println("%x",tree.Root.ToBytes())
}