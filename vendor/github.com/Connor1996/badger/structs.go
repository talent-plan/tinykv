package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/Connor1996/badger/y"
)

// header is used in value log as a header before Entry.
type header struct {
	klen uint32
	vlen uint32
	meta byte

	// umlen is the length of UserMeta
	umlen byte
}

const (
	headerBufSize       = 10
	metaNotEntryEncoded = 0
)

func (h header) Encode(out []byte) {
	y.Assert(len(out) >= headerBufSize)
	// Because meta can never be 0xff, so 0x00 in vlog file indicates there is not an entry.
	out[0] = ^h.meta
	binary.BigEndian.PutUint32(out[1:5], h.klen)
	binary.BigEndian.PutUint32(out[5:9], h.vlen)
	out[9] = h.umlen
}

// Decodes h from buf.
func (h *header) Decode(buf []byte) {
	h.meta = ^buf[0]
	h.klen = binary.BigEndian.Uint32(buf[1:5])
	h.vlen = binary.BigEndian.Uint32(buf[5:9])
	h.umlen = buf[9]
}

func isEncodedHeader(data []byte) bool {
	if len(data) < 1 {
		return false
	}
	return data[0] != metaNotEntryEncoded
}

// Entry provides Key, Value, UserMeta. This struct can be used by the user to set data.
type Entry struct {
	Key       []byte
	Value     []byte
	UserMeta  []byte
	meta      byte
	logOffset logOffset

	// Fields maintained internally.
	offset uint32
}

func (e *Entry) estimateSize() int {
	return len(e.Key) + len(e.Value) + len(e.UserMeta) + 2 // Meta, UserMeta
}

// Encodes e to buf. Returns number of bytes written.
func encodeEntry(e *Entry, buf *bytes.Buffer) (int, error) {
	h := header{
		klen:  uint32(len(e.Key)),
		vlen:  uint32(len(e.Value)),
		meta:  e.meta,
		umlen: byte(len(e.UserMeta)),
	}

	var headerEnc [headerBufSize]byte
	h.Encode(headerEnc[:])

	hash := crc32.New(y.CastagnoliCrcTable)

	buf.Write(headerEnc[:])
	hash.Write(headerEnc[:])

	buf.Write(e.UserMeta)
	hash.Write(e.UserMeta)

	buf.Write(e.Key)
	hash.Write(e.Key)

	buf.Write(e.Value)
	hash.Write(e.Value)

	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	buf.Write(crcBuf[:])

	return len(headerEnc) + len(e.UserMeta) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

func (e Entry) print(prefix string) {
	fmt.Printf("%s Key: %s Meta: %d UserMeta: %v Offset: %d len(val)=%d",
		prefix, e.Key, e.meta, e.UserMeta, e.offset, len(e.Value))
}
