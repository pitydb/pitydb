package page

import (
	"testing"
	"time"
)

type Size uint64
func TestNewPage(t *testing.T) {

	x := time.Now().UnixNano()
	println(int32(x))
	println(Size(x))
}