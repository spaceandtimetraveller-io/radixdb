package radixdb

import (
	"encoding/binary"
	"unsafe"
)

// Row is one administrative row stored under a key (same semantics as benchmark Leaf entries).
// FullPath may alias the mmap-backed database file; it remains valid until the DB is closed or
// the file is remapped (do not mutate the mmap while holding a Row from reads).
type Row struct {
	ParentID int32
	ID       int32
	FullPath string
}

// Leaf chunk format (prepend-linked list; newest chunk first, decode reverses to insertion order):
// [parentId u32][id u32][pathLen u32][path bytes][next u64] next=0 means end of chain (oldest tail).

func encodeChunk(r Row, next uint64) []byte {
	pl := len(r.FullPath)
	b := make([]byte, 12+pl+8)
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.ParentID))
	binary.LittleEndian.PutUint32(b[4:8], uint32(r.ID))
	binary.LittleEndian.PutUint32(b[8:12], uint32(pl))
	copy(b[12:], r.FullPath)
	binary.LittleEndian.PutUint64(b[12+pl:], next)
	return b
}

func decodeChunkAt(data []byte, at int) (r Row, next uint64, err error) {
	if at < 0 || at+12 > len(data) {
		return Row{}, 0, ErrCorrupt
	}
	r.ParentID = int32(binary.LittleEndian.Uint32(data[at : at+4]))
	r.ID = int32(binary.LittleEndian.Uint32(data[at+4 : at+8]))
	pl := int(binary.LittleEndian.Uint32(data[at+8 : at+12]))
	if pl < 0 || at+12+pl+8 > len(data) {
		return Row{}, 0, ErrCorrupt
	}
	if pl == 0 {
		r.FullPath = ""
	} else {
		p := data[at+12 : at+12+pl]
		r.FullPath = unsafe.String(unsafe.SliceData(p), pl)
	}
	next = binary.LittleEndian.Uint64(data[at+12+pl : at+12+pl+8])
	return r, next, nil
}

// peekChunkNext returns the next-chunk offset without decoding the full row (for chain sizing).
func peekChunkNext(data []byte, at int) (next uint64, err error) {
	if at < 0 || at+12 > len(data) {
		return 0, ErrCorrupt
	}
	pl := int(binary.LittleEndian.Uint32(data[at+8 : at+12]))
	if pl < 0 || at+12+pl+8 > len(data) {
		return 0, ErrCorrupt
	}
	return binary.LittleEndian.Uint64(data[at+12+pl : at+12+pl+8]), nil
}

func (db *DB) decodeLeafRows(head uint64) ([]Row, error) {
	if head == 0 {
		return nil, nil
	}
	var n int
	for off := head; off != 0; n++ {
		if int(off) >= len(db.mmap) {
			return nil, ErrCorrupt
		}
		next, err := peekChunkNext(db.mmap, int(off))
		if err != nil {
			return nil, err
		}
		off = next
	}
	rows := make([]Row, n)
	i := n - 1
	for off := head; off != 0; {
		if int(off) >= len(db.mmap) {
			return nil, ErrCorrupt
		}
		r, next, err := decodeChunkAt(db.mmap, int(off))
		if err != nil {
			return nil, err
		}
		rows[i] = r
		i--
		off = next
	}
	return rows, nil
}

func (db *DB) prependLeafChunk(oldHead uint64, r Row) uint64 {
	chunk := encodeChunk(r, oldHead)
	return db.appendBytes(chunk)
}

func encodeLeafSingle(r Row) []byte {
	return encodeChunk(r, 0)
}
