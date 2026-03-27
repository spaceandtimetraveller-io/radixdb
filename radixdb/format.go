package radixdb

import (
	"encoding/binary"
	"errors"
)

const (
	magic   = "MRDX"
	version = uint32(1)
	// HeaderSize is reserved file header; all node offsets are absolute from file start.
	HeaderSize = 4096
	// Header layout: [0:4) magic, [4:8) version, [8:16) rootOff, [16:24) distinctKeys,
	// [24:32) totalRows, [32] statsValid (1 = counters maintained; 0 = legacy / unknown).
	hdrRootOff    = 8
	hdrDistinct   = 16
	hdrTotalRows  = 24
	hdrStatsValid = 32
	minHeader     = 33
)

var (
	ErrMagic     = errors.New("radixdb: bad magic")
	ErrVersion   = errors.New("radixdb: unsupported version")
	ErrCorrupt   = errors.New("radixdb: corrupt node or leaf")
	ErrReadOnly  = errors.New("radixdb: read-only")
	ErrKeyTooBig = errors.New("radixdb: key too large")
)

func putHeader(buf []byte, rootOff uint64) {
	writeHeader(buf, rootOff, 0, 0, true)
}

// writeHeader writes the full fixed header including stats (used by Open empty file and migrations).
func writeHeader(buf []byte, rootOff, distinctKeys, totalRows uint64, statsValid bool) {
	copy(buf[0:4], magic)
	binary.LittleEndian.PutUint32(buf[4:8], version)
	binary.LittleEndian.PutUint64(buf[hdrRootOff:hdrRootOff+8], rootOff)
	binary.LittleEndian.PutUint64(buf[hdrDistinct:hdrDistinct+8], distinctKeys)
	binary.LittleEndian.PutUint64(buf[hdrTotalRows:hdrTotalRows+8], totalRows)
	if statsValid {
		buf[hdrStatsValid] = 1
	} else {
		buf[hdrStatsValid] = 0
	}
}

func parseHeader(buf []byte) (rootOff uint64, err error) {
	if len(buf) < 16 {
		return 0, ErrCorrupt
	}
	if string(buf[0:4]) != magic {
		return 0, ErrMagic
	}
	if binary.LittleEndian.Uint32(buf[4:8]) != version {
		return 0, ErrVersion
	}
	return binary.LittleEndian.Uint64(buf[hdrRootOff : hdrRootOff+8]), nil
}

func parseHeaderStats(buf []byte) (distinctKeys, totalRows uint64, statsValid bool, err error) {
	if len(buf) < minHeader {
		return 0, 0, false, ErrCorrupt
	}
	if string(buf[0:4]) != magic {
		return 0, 0, false, ErrMagic
	}
	if binary.LittleEndian.Uint32(buf[4:8]) != version {
		return 0, 0, false, ErrVersion
	}
	distinctKeys = binary.LittleEndian.Uint64(buf[hdrDistinct : hdrDistinct+8])
	totalRows = binary.LittleEndian.Uint64(buf[hdrTotalRows : hdrTotalRows+8])
	statsValid = buf[hdrStatsValid] == 1
	return distinctKeys, totalRows, statsValid, nil
}
