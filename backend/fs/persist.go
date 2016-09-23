package fs

type Persistent interface {
	//Make the slot type to be bytes
	ToBytes() []byte

	//Read needed bytes from byte array and make itself a Slot object
	Make(buf []byte, offset uint32) uint32
}