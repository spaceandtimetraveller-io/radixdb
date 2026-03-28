package radixdb

import (
	"encoding/binary"
	"unsafe"
)

// Row is one administrative row stored under a key (same semantics as radixdb).
// FullPath may alias block memory; do not retain past the enclosing read or after Close.
type Row struct {
	ParentID int32
	ID       int32
	FullPath string
}

func encodeChunk(r Row, next Ref) []byte {
	pl := len(r.FullPath)
	b := make([]byte, 12+pl+8)
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.ParentID))
	binary.LittleEndian.PutUint32(b[4:8], uint32(r.ID))
	binary.LittleEndian.PutUint32(b[8:12], uint32(pl))
	copy(b[12:], r.FullPath)
	binary.LittleEndian.PutUint64(b[12+pl:], uint64(next))
	return b
}

func decodeChunkAt(data []byte, at int) (r Row, next Ref, err error) {
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
	next = Ref(binary.LittleEndian.Uint64(data[at+12+pl : at+12+pl+8]))
	return r, next, nil
}

// decodeLeafRowsChain walks a prepend-linked leaf chain; chunks may live in different blocks.
// One disk read per chunk (prepend order is reversed to match logical row order).
func decodeLeafRowsChain(bm *blockMgr, head Ref) ([]Row, error) {
	if head == 0 {
		return nil, nil
	}
	var rows []Row
	for r := head; r != 0; {
		if r.Block() == 0 && r.Offset() < headerUsedV2 {
			return nil, ErrCorrupt
		}
		block, err := bm.readBlock(r.Block())
		if err != nil {
			return nil, err
		}
		off := int(r.Offset())
		row, next, err := decodeChunkAt(block, off)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		r = next
	}
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows, nil
}

func encodeLeafSingle(r Row) []byte {
	return encodeChunk(r, 0)
}
