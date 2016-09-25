package row

import (
	"github.com/stretchr/testify/assert"
	"github.com/lycying/pitydb/backend/fs/slot"
	"testing"
)

func TestRowDef_MakeSlot_ToBytes(t *testing.T) {
	data := []byte{}
	data = append(data, slot.NewString("I am a girl!").ToBytes()...)
	data = append(data, slot.NewInteger(int32(-1024)).ToBytes()...)
	data = append(data, slot.NewString("迷途知返").ToBytes()...)
	data = append(data, slot.NewDouble(float64(9299.29129032424239423423423422424)).ToBytes()...)
	data = append(data, slot.NewBoolean(true).ToBytes()...)
	data = append(data, slot.NewString("!@#$%^&*()END").ToBytes()...)
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

	root := NewRow(meta)
	root.Make(data, 0)
	assert.Equal(t, root.Data[1].(*slot.Integer).Val, int32(-1024), "should be eq")
	assert.Equal(t, root.Data[5].(*slot.String).Val, "!@#$%^&*()END", "should be eq")
	assert.Equal(t, root.ToBytes(), data, "should be eq")
}