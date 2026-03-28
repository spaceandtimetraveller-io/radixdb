package radixdb

import (
	"encoding/binary"
)

const (
	magic = "RDX2"

	// Version1 is the original on-disk layout (header 48 bytes).
	Version1 uint32 = 1
	// Version2 adds compaction tuning fields after byte 48.
	Version2 uint32 = 2

	// BlockSize is the fixed page size (DuckDB-style block addressing).
	BlockSize = 4096

	// HeaderBlock is always block index 0.
	HeaderBlock = 0

	hdrMagicOff      = 0
	hdrVersionOff    = 4
	hdrRootRefOff    = 8
	hdrNBlocksOff    = 16
	hdrAllocBlockOff = 20
	hdrAllocOffOff   = 24
	hdrDistinctOff   = 28
	hdrTotalRowsOff  = 36
	hdrStatsValidOff = 44
	headerUsedV1     = 48

	hdrCompactCooldownOff      = 48
	hdrCompactMinReclaimMBOff  = 52
	hdrCompactWasteRatioPctOff = 56
	hdrCompactMinFileMBOff     = 60
	headerUsedV2               = 64
)

// Ref is a swizzle-style disk reference: (block index, byte offset within block).
// 0 means nil. All tree pointers use this packed form on disk and in memory.
type Ref uint64

func RefNil() Ref { return 0 }

func PackRef(block uint32, off uint32) Ref {
	if off >= BlockSize {
		panic("radixdb: offset >= BlockSize")
	}
	return Ref(uint64(block)<<12 | uint64(off))
}

func (r Ref) Valid() bool { return r != 0 }

func (r Ref) Block() uint32 { return uint32(uint64(r) >> 12) }

func (r Ref) Offset() uint32 { return uint32(uint64(r) & 0xfff) }

// FileHeader is the decoded block-0 header (v1 or v2).
type FileHeader struct {
	Version              uint32
	Root                 Ref
	NBlocks              uint32
	AllocBlock           uint32
	AllocOff             uint32
	Distinct             uint64
	Total                uint64
	StatsValid           bool
	CompactCooldownSec   uint32
	CompactMinReclaimMB  uint32
	CompactWasteRatioPct uint32
	CompactMinFileMB     uint32
}

func parseHeaderBlock(data []byte) (FileHeader, error) {
	var h FileHeader
	if len(data) < headerUsedV1 {
		return h, ErrCorrupt
	}
	if string(data[hdrMagicOff:hdrMagicOff+4]) != magic {
		return h, ErrMagic
	}
	h.Version = binary.LittleEndian.Uint32(data[hdrVersionOff : hdrVersionOff+4])
	if h.Version != Version1 && h.Version != Version2 {
		return h, ErrVersion
	}
	h.Root = Ref(binary.LittleEndian.Uint64(data[hdrRootRefOff : hdrRootRefOff+8]))
	h.NBlocks = binary.LittleEndian.Uint32(data[hdrNBlocksOff : hdrNBlocksOff+4])
	h.AllocBlock = binary.LittleEndian.Uint32(data[hdrAllocBlockOff : hdrAllocBlockOff+4])
	h.AllocOff = binary.LittleEndian.Uint32(data[hdrAllocOffOff : hdrAllocOffOff+4])
	h.Distinct = binary.LittleEndian.Uint64(data[hdrDistinctOff : hdrDistinctOff+8])
	h.Total = binary.LittleEndian.Uint64(data[hdrTotalRowsOff : hdrTotalRowsOff+8])
	h.StatsValid = data[hdrStatsValidOff] == 1

	if h.Version == Version2 {
		if len(data) < headerUsedV2 {
			return h, ErrCorrupt
		}
		h.CompactCooldownSec = binary.LittleEndian.Uint32(data[hdrCompactCooldownOff : hdrCompactCooldownOff+4])
		h.CompactMinReclaimMB = binary.LittleEndian.Uint32(data[hdrCompactMinReclaimMBOff : hdrCompactMinReclaimMBOff+4])
		h.CompactWasteRatioPct = binary.LittleEndian.Uint32(data[hdrCompactWasteRatioPctOff : hdrCompactWasteRatioPctOff+4])
		h.CompactMinFileMB = binary.LittleEndian.Uint32(data[hdrCompactMinFileMBOff : hdrCompactMinFileMBOff+4])
	}
	return h, nil
}

func writeHeaderBlockV2(data []byte, h FileHeader) {
	copy(data[hdrMagicOff:hdrMagicOff+4], magic)
	binary.LittleEndian.PutUint32(data[hdrVersionOff:hdrVersionOff+4], Version2)
	binary.LittleEndian.PutUint64(data[hdrRootRefOff:hdrRootRefOff+8], uint64(h.Root))
	binary.LittleEndian.PutUint32(data[hdrNBlocksOff:hdrNBlocksOff+4], h.NBlocks)
	binary.LittleEndian.PutUint32(data[hdrAllocBlockOff:hdrAllocBlockOff+4], h.AllocBlock)
	binary.LittleEndian.PutUint32(data[hdrAllocOffOff:hdrAllocOffOff+4], h.AllocOff)
	binary.LittleEndian.PutUint64(data[hdrDistinctOff:hdrDistinctOff+8], h.Distinct)
	binary.LittleEndian.PutUint64(data[hdrTotalRowsOff:hdrTotalRowsOff+8], h.Total)
	if h.StatsValid {
		data[hdrStatsValidOff] = 1
	} else {
		data[hdrStatsValidOff] = 0
	}
	binary.LittleEndian.PutUint32(data[hdrCompactCooldownOff:hdrCompactCooldownOff+4], h.CompactCooldownSec)
	binary.LittleEndian.PutUint32(data[hdrCompactMinReclaimMBOff:hdrCompactMinReclaimMBOff+4], h.CompactMinReclaimMB)
	binary.LittleEndian.PutUint32(data[hdrCompactWasteRatioPctOff:hdrCompactWasteRatioPctOff+4], h.CompactWasteRatioPct)
	binary.LittleEndian.PutUint32(data[hdrCompactMinFileMBOff:hdrCompactMinFileMBOff+4], h.CompactMinFileMB)
}
