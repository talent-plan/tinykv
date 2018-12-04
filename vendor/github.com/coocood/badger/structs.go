package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/coocood/badger/y"
)

type valuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint32
}

func (p valuePointer) Less(o valuePointer) bool {
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	return p.Len < o.Len
}

func (p valuePointer) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

const vptrSize = 12

// Encode encodes Pointer into byte buffer.
func (p valuePointer) Encode(b []byte) []byte {
	binary.BigEndian.PutUint32(b[:4], p.Fid)
	binary.BigEndian.PutUint32(b[4:8], p.Len)
	binary.BigEndian.PutUint32(b[8:12], p.Offset)
	return b[:vptrSize]
}

func (p *valuePointer) Decode(b []byte) {
	p.Fid = binary.BigEndian.Uint32(b[:4])
	p.Len = binary.BigEndian.Uint32(b[4:8])
	p.Offset = binary.BigEndian.Uint32(b[8:12])
}

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
	Key      []byte
	Value    []byte
	UserMeta []byte
	meta     byte

	// Fields maintained internally.
	offset uint32
}

func (e *Entry) estimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + len(e.UserMeta) + 2 // Meta, UserMeta
	}
	return len(e.Key) + len(e.UserMeta) + 12 + 2 // 12 for ValuePointer, 2 for metas.
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
