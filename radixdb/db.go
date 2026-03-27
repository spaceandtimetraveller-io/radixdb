package radixdb

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"artbenchmark/radixdb/bytecmp"

	"golang.org/x/sys/unix"
)

// DB is a mmap-backed radix tree with copy-on-write inserts and lock-free reads.
type DB struct {
	mu       sync.Mutex // serializes writes only; readers do not take this
	f        *os.File
	mmap     []byte
	readOnly bool
	root     atomic.Uint64

	statDistinct atomic.Uint64
	statTotal    atomic.Uint64
	// headerStatsValid is true when mmap[hdrStatsValid] was 1 at open (counters trusted on disk).
	headerStatsValid bool
	// roStatsMu / legacyROStatsFilled protect one-time walk for read-only DBs without valid header stats.
	roStatsMu           sync.Mutex
	legacyROStatsFilled bool
}

// Open maps path read/write. The file is created if missing and truncated to HeaderSize if empty.
func Open(path string) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := st.Size()
	if size < HeaderSize {
		if err := f.Truncate(HeaderSize); err != nil {
			f.Close()
			return nil, err
		}
		size = HeaderSize
		hdr := make([]byte, HeaderSize)
		putHeader(hdr, 0)
		if _, err := f.WriteAt(hdr, 0); err != nil {
			f.Close()
			return nil, err
		}
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	root, err := parseHeader(data)
	if err != nil {
		unix.Munmap(data)
		f.Close()
		return nil, err
	}
	distinct, total, statsValid, err := parseHeaderStats(data)
	if err != nil {
		unix.Munmap(data)
		f.Close()
		return nil, err
	}
	db := &DB{f: f, mmap: data, headerStatsValid: statsValid}
	db.root.Store(root)
	if root != 0 && !statsValid {
		dk, tr, werr := db.statsByWalk()
		if werr != nil {
			unix.Munmap(data)
			f.Close()
			return nil, werr
		}
		writeHeader(data, root, dk, tr, true)
		db.statDistinct.Store(dk)
		db.statTotal.Store(tr)
		db.headerStatsValid = true
	} else if statsValid {
		db.statDistinct.Store(distinct)
		db.statTotal.Store(total)
	}
	return db, nil
}

// OpenReadOnly opens an existing database mmap read-only.
func OpenReadOnly(path string) (*DB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() < HeaderSize {
		f.Close()
		return nil, ErrCorrupt
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	root, err := parseHeader(data)
	if err != nil {
		unix.Munmap(data)
		f.Close()
		return nil, err
	}
	distinct, total, statsValid, err := parseHeaderStats(data)
	if err != nil {
		unix.Munmap(data)
		f.Close()
		return nil, err
	}
	db := &DB{f: f, mmap: data, readOnly: true, headerStatsValid: statsValid}
	db.root.Store(root)
	if statsValid {
		db.statDistinct.Store(distinct)
		db.statTotal.Store(total)
	}
	return db, nil
}

// RootOffset returns the current radix root offset in the mapped file (diagnostics).
func (db *DB) RootOffset() uint64 {
	return db.root.Load()
}

// Close unmmaps and closes the file.
func (db *DB) Close() error {
	if db.mmap != nil {
		_ = unix.Munmap(db.mmap)
		db.mmap = nil
	}
	if db.f != nil {
		err := db.f.Close()
		db.f = nil
		return err
	}
	return nil
}

func (db *DB) flushHeader() {
	if len(db.mmap) < minHeader {
		return
	}
	binary.LittleEndian.PutUint64(db.mmap[hdrRootOff:hdrRootOff+8], db.root.Load())
	binary.LittleEndian.PutUint64(db.mmap[hdrDistinct:hdrDistinct+8], db.statDistinct.Load())
	binary.LittleEndian.PutUint64(db.mmap[hdrTotalRows:hdrTotalRows+8], db.statTotal.Load())
	db.mmap[hdrStatsValid] = 1
}

func (db *DB) ensure(min int64) error {
	if min < 0 {
		return errors.New("radixdb: invalid allocation size")
	}
	if int64(len(db.mmap)) >= min {
		return nil
	}
	size := int64(len(db.mmap))
	if size == 0 {
		return errors.New("radixdb: mmap length zero")
	}
	for size < min {
		next := size * 2
		if next <= size || next > min {
			next = min
		}
		if next < min {
			next = min
		}
		size = next
	}
	if err := db.f.Truncate(size); err != nil {
		return err
	}
	_ = unix.Munmap(db.mmap)
	data, err := unix.Mmap(int(db.f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return err
	}
	db.mmap = data
	return nil
}

func (db *DB) appendBytes(p []byte) uint64 {
	cur := uint64(len(db.mmap))
	need := cur + uint64(len(p))
	if need < cur {
		panic("radixdb: allocation overflow")
	}
	if need > uint64(math.MaxInt64) {
		panic("radixdb: allocation overflow")
	}
	if err := db.ensure(int64(need)); err != nil {
		panic(err)
	}
	copy(db.mmap[cur:], p)
	return cur
}

func (db *DB) writeNode(n *decodedNode) uint64 {
	return db.appendBytes(encodeNode(n))
}

func (db *DB) loadNode(off uint64) (*decodedNode, error) {
	if off == 0 || int(off) >= len(db.mmap) {
		return nil, ErrCorrupt
	}
	return decodeNode(db.mmap, int(off))
}

// Insert adds a row under key (duplicate keys append to the same leaf).
func (db *DB) Insert(key string, r Row) error {
	if db.readOnly {
		return ErrReadOnly
	}
	k := []byte(key)
	db.mu.Lock()
	defer db.mu.Unlock()

	root := db.root.Load()
	if root == 0 {
		empty := &decodedNode{}
		root = db.writeNode(empty)
		db.root.Store(root)
	}
	newRoot, newDistinct, err := db.insert(root, k, k, r)
	if err != nil {
		return err
	}
	db.root.Store(newRoot)
	db.statTotal.Add(1)
	if newDistinct {
		db.statDistinct.Add(1)
	}
	db.flushHeader()
	return nil
}

// Sync fsyncs the backing file (durability of header and appended data).
func (db *DB) Sync() error {
	if db.f == nil {
		return nil
	}
	db.flushHeader()
	return db.f.Sync()
}

// Get returns all rows for an exact key.
func (db *DB) Get(key string) ([]Row, bool, error) {
	off := db.root.Load()
	if off == 0 {
		return nil, false, nil
	}
	k := []byte(key)
	leafOff, ok := db.seekLeaf(off, k)
	if !ok || leafOff == 0 {
		return nil, false, nil
	}
	rows, err := db.decodeLeafRows(leafOff)
	if err != nil {
		return nil, false, err
	}
	return rows, true, nil
}

// seekLeaf finds the leaf offset for exact key k (iradix Node.Get logic).
func (db *DB) seekLeaf(off uint64, k []byte) (uint64, bool) {
	search := k
	for {
		n, err := db.loadNode(off)
		if err != nil {
			return 0, false
		}
		if len(search) == 0 {
			if n.leaf != 0 {
				return n.leaf, true
			}
			return 0, false
		}
		_, _, childOff := n.getEdge(search[0])
		if childOff == 0 {
			return 0, false
		}
		child, err := db.loadNode(childOff)
		if err != nil {
			return 0, false
		}
		if !bytecmp.HasPrefixBytes(search, child.prefix) {
			return 0, false
		}
		search = search[len(child.prefix):]
		off = childOff
	}
}

// recursiveWalk visits all keys under off; buf holds the path from root to the start of this node's prefix
// (reused across recursion to avoid per-node key allocations).
func (db *DB) recursiveWalk(off uint64, buf []byte, fn func(key []byte, rows []Row) bool) error {
	n, err := db.loadNode(off)
	if err != nil {
		return err
	}
	start := len(buf)
	buf = append(buf, n.prefix...)
	nodeEnd := len(buf)
	if n.leaf != 0 {
		rows, err := db.decodeLeafRows(n.leaf)
		if err != nil {
			buf = buf[:start]
			return err
		}
		if fn(buf, rows) {
			buf = buf[:start]
			return nil
		}
	}
	for _, e := range n.edges {
		if err := db.recursiveWalk(e.off, buf, fn); err != nil {
			buf = buf[:start]
			return err
		}
		buf = buf[:nodeEnd]
	}
	buf = buf[:start]
	return nil
}

// WalkPrefixBytes calls fn for every key whose byte representation has prefix prefix.
func (db *DB) WalkPrefixBytes(prefix []byte, fn func(key []byte, rows []Row) bool) error {
	return db.walkPrefixBytesNoStats(prefix, fn)
}

// WalkPrefix calls fn for every key whose byte representation has prefix prefix.
// Each key string is a distinct copy (safe to retain). For zero allocation per key, use WalkPrefixBytes
// and do not retain the []byte key past the callback without copying.
func (db *DB) WalkPrefix(prefix string, fn func(key string, rows []Row) bool) error {
	return db.WalkPrefixBytes([]byte(prefix), func(key []byte, rows []Row) bool {
		return fn(string(key), rows)
	})
}

// statsByWalk counts keys/rows by traversing the tree (used for migration and read-only legacy open).
func (db *DB) statsByWalk() (uint64, uint64, error) {
	var distinctKeys, totalRows uint64
	err := db.walkPrefixBytesNoStats(nil, func(_ []byte, rows []Row) bool {
		distinctKeys++
		totalRows += uint64(len(rows))
		return false
	})
	return distinctKeys, totalRows, err
}

func (db *DB) walkPrefixBytesNoStats(prefix []byte, fn func(key []byte, rows []Row) bool) error {
	off := db.root.Load()
	if off == 0 {
		return nil
	}
	search := prefix
	before := make([]byte, 0, len(prefix)+64)
	for {
		n, err := db.loadNode(off)
		if err != nil {
			return err
		}
		if len(search) == 0 {
			return db.recursiveWalk(off, before, fn)
		}
		_, _, childOff := n.getEdge(search[0])
		if childOff == 0 {
			return nil
		}
		child, err := db.loadNode(childOff)
		if err != nil {
			return err
		}
		if bytecmp.HasPrefixBytes(search, child.prefix) {
			before = append(before, n.prefix...)
			search = search[len(child.prefix):]
			off = childOff
			continue
		}
		if bytecmp.HasPrefixBytes(child.prefix, search) {
			before = append(before, n.prefix...)
			return db.recursiveWalk(childOff, before, fn)
		}
		return nil
	}
}

// Stats returns the number of distinct keys with at least one row and the total row count
// (including multiple rows under the same key). O(1) when header stats are valid; otherwise
// read-only databases without valid on-disk stats perform a one-time full walk on first call.
func (db *DB) Stats() (uint64, uint64, error) {
	if db.readOnly && !db.headerStatsValid && db.root.Load() != 0 {
		db.roStatsMu.Lock()
		if !db.legacyROStatsFilled {
			dk, tr, err := db.statsByWalk()
			if err != nil {
				db.roStatsMu.Unlock()
				return 0, 0, err
			}
			db.statDistinct.Store(dk)
			db.statTotal.Store(tr)
			db.legacyROStatsFilled = true
		}
		db.roStatsMu.Unlock()
	}
	return db.statDistinct.Load(), db.statTotal.Load(), nil
}
