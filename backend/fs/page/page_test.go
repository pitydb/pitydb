package page

import (
	"testing"
	"github.com/lycying/pitydb/backend/fs/slot"
	"os"
	"github.com/stretchr/testify/assert"
)

func getClusterKeyArrayFromRows(page Page) []uint32 {
	debugarr := []uint32{}

	if page.Runtime().Type.Value == TYPE_DATA_PAGE {
		v := page.(*DataPage)
		for _, rowinfo := range v.Content {
			debugarr = append(debugarr, rowinfo.ClusteredKey.Value)
		}
	}
	return debugarr
}
func TestNewPage(t *testing.T) {
	meta := &RowMeta{
		Type:slot.ST_ROOT,
		Children:[]*RowMeta{
			&RowMeta{Type:slot.ST_STRING},
			&RowMeta{Type:slot.ST_INTEGER},
			&RowMeta{Type:slot.ST_STRING},
			&RowMeta{Type:slot.ST_DOUBLE},
			&RowMeta{Type:slot.ST_BOOL},
			&RowMeta{Type:slot.ST_STRING},
		},
	}
	link, _ := os.OpenFile("/tmp/b", os.O_RDWR, 0666)
	tree := NewPageTree(meta, link)

	for i := 1; i <= 2; i++ {
		r := NewRow(meta)
		r.Fill(
			slot.NewString("skflksfsfdsjflsjfslfj"),
			slot.NewInteger(int32(100)),
			slot.NewString("内地"),
			slot.NewDouble(float64(0.234242423423)),
			slot.NewBoolean(true),
			slot.NewString("卡卡老师封疆大吏舒服的沙发"),
		)
		r.ClusteredKey = slot.NewUnsignedInteger(uint32(i * 2))

		tree.InsertRow(r)
	}
	assert.Equal(t, getClusterKeyArrayFromRows(tree.root), []uint32{uint32(2), uint32(4)})
	for i := 1; i <= 2; i++ {
		r := NewRow(meta)
		r.Fill(
			slot.NewString("skflksfsfdsjflsjfslfj"),
			slot.NewInteger(int32(100)),
			slot.NewString("内地"),
			slot.NewDouble(float64(0.234242423423)),
			slot.NewBoolean(true),
			slot.NewString("卡卡老师封疆大吏舒服的沙发"),
		)
		r.ClusteredKey = slot.NewUnsignedInteger(uint32(i * 2))

		tree.InsertRow(r)
	}

	assert.Equal(t, getClusterKeyArrayFromRows(tree.root), []uint32{uint32(2), uint32(4)})
	for i := 4; i > 0; i-- {
		r := NewRow(meta)
		r.Fill(
			slot.NewString("skflksfsfdsjflsjfslfj"),
			slot.NewInteger(int32(100)),
			slot.NewString("内地"),
			slot.NewDouble(float64(0.234242423423)),
			slot.NewBoolean(true),
			slot.NewString("卡卡老师封疆大吏舒服的沙发"),
		)
		r.ClusteredKey = slot.NewUnsignedInteger(uint32(i * 2 + 1))

		tree.InsertRow(r)
	}
	assert.Equal(t, getClusterKeyArrayFromRows(tree.root), []uint32{uint32(2), uint32(3), uint32(4), uint32(5), uint32(7), uint32(9)})
	tree.Delete(uint32(1))
	assert.Equal(t, getClusterKeyArrayFromRows(tree.root), []uint32{uint32(2), uint32(3), uint32(4), uint32(5), uint32(7), uint32(9)})
	tree.Delete(uint32(2))
	assert.Equal(t, getClusterKeyArrayFromRows(tree.root), []uint32{uint32(3), uint32(4), uint32(5), uint32(7), uint32(9)})
	tree.Delete(uint32(5))
	assert.Equal(t, getClusterKeyArrayFromRows(tree.root), []uint32{uint32(3), uint32(4), uint32(7), uint32(9)})

	for i := 1; i < 10000; i++ {
		r := NewRow(meta)
		r.Fill(
			slot.NewString("skflksfsfdsjflsjfslfj"),
			slot.NewInteger(int32(100)),
			slot.NewString("内地"),
			slot.NewDouble(float64(0.234242423423)),
			slot.NewBoolean(true),
			slot.NewString("卡卡老师封疆大吏舒服的沙发"),
		)
		r.ClusteredKey = slot.NewUnsignedInteger(uint32(i))

		tree.InsertRow(r)
	}
	tree.Dump(1)
	for i := 1; i < 10000; i++ {
		_, _, found := tree.FindRow(uint32(i))
		assert.Equal(t, found, true)
	}

}
