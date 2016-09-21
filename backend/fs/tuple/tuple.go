package tuple

import "github.com/lycying/laladb/backend/fs/slot"


//notice! we support list,hash,set,json etc . So the the tuple desc may have deep level
//To avoid performance or design problem, Tuple only support 128 level
type TupleDesc struct {
	Type  byte //type of the tuple,if it is zero , then it's the root tuple
	Items []*TupleDesc
}


//最小的元组存储单元，元组不能跨数据快存储
type Tuple struct {
	Pre   *Tuple
	Next  *Tuple
	Items []*slot.Slot
}

type PersistTuple struct {
	Key  int
	Pre  int
	Next int
}

