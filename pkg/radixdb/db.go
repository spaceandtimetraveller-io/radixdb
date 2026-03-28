package radixdb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"artbenchmark/pkg/bytecmp"
)

// DB is a block-paged radix tree (DuckDB-style addressing: Ref = block<<12 | offset).
// Inserts are serialized on mu; root is published after all allocations for that insert complete.
// The buffer manager caches whole blocks; old mappings are not invalidated on growth.
// readCloseMu pairs concurrent readers with Close (exclusive drain before teardown).
type DB struct {
	mu          sync.Mutex
	readCloseMu sync.RWMutex
	f           *os.File
	bm          *blockMgr
	readOnly    bool
	path        string

	root atomic.Uint64 // Ref

	statDistinct atomic.Uint64
	statTotal    atomic.Uint64
	statsValid   atomic.Uint32

	nBlocks    atomic.Uint32 // block count; atomic for lock-free consistency checks vs root
	allocBlock uint32
	allocOff   uint32

	compactCooldownSec        uint32
	compactMinReclaimMB       uint32
	compactWasteRatioPct      uint32
	compactMinFileMB          uint32
	compactionInFlight        bool
	compactionLastRunUnixNano atomic.Int64

	// writeSeq increments on each successful Insert; lastCompactWriteSeq is writeSeq at end of last successful CompactIfNeeded.
	writeSeq            atomic.Uint64
	lastCompactWriteSeq atomic.Uint64
}

func align8(n int) int {
	return (n + 7) &^ 7
}

// Open creates or opens an RDX2 radixdb database file (block-structured).
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
	bm := newBlockMgr(f)
	db := &DB{f: f, bm: bm, path: filepath.Clean(path)}

	if st.Size() == 0 {
		if err := bm.ensureNBlocks(1); err != nil {
			f.Close()
			return nil, err
		}
		db.applyCompactionFromHeader(FileHeader{})
		h := FileHeader{
			Version:              Version2,
			NBlocks:              1,
			AllocBlock:           1,
			StatsValid:           true,
			CompactCooldownSec:   db.compactCooldownSec,
			CompactMinReclaimMB:  db.compactMinReclaimMB,
			CompactWasteRatioPct: db.compactWasteRatioPct,
			CompactMinFileMB:     db.compactMinFileMB,
		}
		hdr := make([]byte, BlockSize)
		writeHeaderBlockV2(hdr, h)
		if err := bm.writeBlock(0, hdr); err != nil {
			f.Close()
			return nil, err
		}
		db.nBlocks.Store(1)
		db.allocBlock = 1
		db.allocOff = 0
		db.statsValid.Store(1)
		return db, nil
	}

	if st.Size()%int64(BlockSize) != 0 {
		f.Close()
		return nil, ErrCorrupt
	}
	nbFromFile := uint32(st.Size() / int64(BlockSize))
	if nbFromFile == 0 {
		f.Close()
		return nil, ErrCorrupt
	}
	hdr, err := bm.readBlock(0)
	if err != nil {
		f.Close()
		return nil, err
	}
	h, err := parseHeaderBlock(hdr)
	if err != nil {
		f.Close()
		return nil, err
	}
	if h.NBlocks != nbFromFile {
		f.Close()
		return nil, ErrCorrupt
	}
	if err := db.applyHeader(h); err != nil {
		f.Close()
		return nil, err
	}
	return db, nil
}

// OpenReadOnly opens an existing database for reads only.
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
	if st.Size() < int64(BlockSize) || st.Size()%int64(BlockSize) != 0 {
		f.Close()
		return nil, ErrCorrupt
	}
	bm := newBlockMgr(f)
	db := &DB{f: f, bm: bm, readOnly: true, path: filepath.Clean(path)}
	nbFromFile := uint32(st.Size() / int64(BlockSize))
	hdr, err := bm.readBlock(0)
	if err != nil {
		f.Close()
		return nil, err
	}
	h, err := parseHeaderBlock(hdr)
	if err != nil {
		f.Close()
		return nil, err
	}
	if h.NBlocks != nbFromFile {
		f.Close()
		return nil, ErrCorrupt
	}
	if err := db.applyHeader(h); err != nil {
		f.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) flushHeader() {
	if db.readOnly || db.f == nil {
		return
	}
	hdr := make([]byte, BlockSize)
	h := FileHeader{
		Version:              Version2,
		Root:                 Ref(db.root.Load()),
		NBlocks:              db.nBlocks.Load(),
		AllocBlock:           db.allocBlock,
		AllocOff:             db.allocOff,
		Distinct:             db.statDistinct.Load(),
		Total:                db.statTotal.Load(),
		StatsValid:           db.statsValid.Load() == 1,
		CompactCooldownSec:   db.compactCooldownSec,
		CompactMinReclaimMB:  db.compactMinReclaimMB,
		CompactWasteRatioPct: db.compactWasteRatioPct,
		CompactMinFileMB:     db.compactMinFileMB,
	}
	writeHeaderBlockV2(hdr, h)
	_ = db.bm.writeBlock(0, hdr)
}

func (db *DB) extendIfNeeded(target uint32) error {
	if target <= db.nBlocks.Load() {
		return nil
	}
	if err := db.bm.ensureNBlocks(target); err != nil {
		return err
	}
	db.nBlocks.Store(target)
	return nil
}

// allocRaw writes p into the current bump region; returns Ref to start (8-byte aligned slot).
func (db *DB) allocRaw(p []byte) (Ref, error) {
	if db.readOnly {
		return 0, ErrReadOnly
	}
	need := align8(len(p))
	if need > BlockSize {
		return 0, ErrKeyTooBig
	}
	for {
		if db.allocBlock < 1 {
			db.allocBlock = 1
		}
		if db.allocBlock >= db.nBlocks.Load() {
			if err := db.extendIfNeeded(db.nBlocks.Load() + 1); err != nil {
				return 0, err
			}
		}
		if db.allocOff+uint32(need) <= BlockSize {
			blk, err := db.bm.readBlock(db.allocBlock)
			if err != nil {
				return 0, err
			}
			ref := PackRef(db.allocBlock, db.allocOff)
			copy(blk[db.allocOff:], p)
			for i := len(p); i < need; i++ {
				blk[db.allocOff+uint32(i)] = 0
			}
			if err := db.bm.writeBlock(db.allocBlock, blk); err != nil {
				return 0, err
			}
			db.allocOff += uint32(need)
			if db.allocOff == uint32(BlockSize) {
				db.allocBlock++
				db.allocOff = 0
			}
			return ref, nil
		}
		db.allocBlock++
		db.allocOff = 0
	}
}

func (db *DB) loadNodeRef(r Ref) (*decodedNode, error) {
	if r == 0 {
		return nil, ErrCorrupt
	}
	blk, err := db.bm.readBlock(r.Block())
	if err != nil {
		return nil, err
	}
	off := int(r.Offset())
	n := &decodedNode{}
	if err := decodeNodeInto(n, blk, off); err != nil {
		return nil, err
	}
	return n, nil
}

func (db *DB) writeNode(n *decodedNode) (Ref, error) {
	b := encodeNode(n)
	return db.allocRaw(b)
}

// Insert adds a row under key (COW append-only).
func (db *DB) Insert(key string, r Row) error {
	if db.readOnly {
		return ErrReadOnly
	}
	k := []byte(key)
	db.mu.Lock()
	defer db.mu.Unlock()

	root := Ref(db.root.Load())
	if root == 0 {
		empty := &decodedNode{}
		nr, err := db.writeNode(empty)
		if err != nil {
			return err
		}
		db.root.Store(uint64(nr))
		root = nr
	}
	newRoot, newDistinct, err := db.insert(root, k, k, r)
	if err != nil {
		return err
	}
	db.root.Store(uint64(newRoot))
	db.statTotal.Add(1)
	if newDistinct {
		db.statDistinct.Add(1)
	}
	db.flushHeader()
	db.writeSeq.Add(1)
	return nil
}

// Get returns rows for an exact key.
func (db *DB) Get(key string) ([]Row, bool, error) {
	db.readCloseMu.RLock()
	defer db.readCloseMu.RUnlock()

	k := []byte(key)
	for spin := 0; spin < 1024; spin++ {
		root := Ref(db.root.Load())
		if root == 0 {
			return nil, false, nil
		}
		if root.Block() >= db.nBlocks.Load() {
			continue
		}
		leaf, ok, err := seekLeafRef(db.bm, root, k)
		if err != nil {
			return nil, false, err
		}
		if !ok || leaf == 0 {
			return nil, false, nil
		}
		rows, err := decodeLeafRowsChain(db.bm, leaf)
		if err != nil {
			return nil, false, err
		}
		return rows, true, nil
	}
	return nil, false, ErrCorrupt
}

func seekLeafRef(bm *blockMgr, root Ref, k []byte) (Ref, bool, error) {
	search := k
	off := root
	var n, child decodedNode
	if err := loadNodeInto(bm, off, &n); err != nil {
		return 0, false, err
	}
	for {
		if len(search) == 0 {
			if n.leaf != 0 {
				return n.leaf, true, nil
			}
			return 0, false, nil
		}
		_, _, childRef := n.getEdge(search[0])
		if childRef == 0 {
			return 0, false, nil
		}
		if err := loadNodeInto(bm, childRef, &child); err != nil {
			return 0, false, err
		}
		if !bytecmp.HasPrefixBytes(search, child.prefix) {
			return 0, false, nil
		}
		search = search[len(child.prefix):]
		off = childRef
		n, child = child, n
	}
}

func loadNodeInto(bm *blockMgr, r Ref, n *decodedNode) error {
	if r == 0 {
		return ErrCorrupt
	}
	blk, err := bm.readBlock(r.Block())
	if err != nil {
		return err
	}
	return decodeNodeInto(n, blk, int(r.Offset()))
}

// WalkPrefix visits keys with the given string prefix.
func (db *DB) WalkPrefix(prefix string, fn func(key string, rows []Row) bool) error {
	return db.WalkPrefixBytes([]byte(prefix), func(k []byte, rows []Row) bool {
		return fn(string(k), rows)
	})
}

// WalkPrefixBytes visits keys with the given byte prefix. Returning true from fn stops the entire walk immediately (including keys in sibling branches).
func (db *DB) WalkPrefixBytes(prefix []byte, fn func(key []byte, rows []Row) bool) error {
	db.readCloseMu.RLock()
	defer db.readCloseMu.RUnlock()
	return db.walkPrefixBytesUnlocked(prefix, fn)
}

func (db *DB) walkPrefixBytesUnlocked(prefix []byte, fn func(key []byte, rows []Row) bool) error {
	for spin := 0; spin < 1024; spin++ {
		root := Ref(db.root.Load())
		if root == 0 {
			return nil
		}
		if root.Block() >= db.nBlocks.Load() {
			continue
		}
		err := walkPrefixFrom(db.bm, root, prefix, fn)
		if errors.Is(err, errWalkStop) {
			return nil
		}
		return err
	}
	return ErrCorrupt
}

func walkPrefixFrom(bm *blockMgr, root Ref, prefix []byte, fn func(key []byte, rows []Row) bool) error {
	off := root
	search := prefix
	before := make([]byte, 0, len(prefix)+64)
	var n, child decodedNode
	if err := loadNodeInto(bm, off, &n); err != nil {
		return err
	}
	for {
		if len(search) == 0 {
			return recursiveWalkRef(bm, off, before, fn, &n)
		}
		_, _, childOff := n.getEdge(search[0])
		if childOff == 0 {
			return nil
		}
		if err := loadNodeInto(bm, childOff, &child); err != nil {
			return err
		}
		if bytecmp.HasPrefixBytes(search, child.prefix) {
			before = append(before, n.prefix...)
			search = search[len(child.prefix):]
			off = childOff
			n, child = child, n
			continue
		}
		if bytecmp.HasPrefixBytes(child.prefix, search) {
			before = append(before, n.prefix...)
			return recursiveWalkRef(bm, childOff, before, fn, &child)
		}
		return nil
	}
}

func recursiveWalkRef(bm *blockMgr, nodeRef Ref, buf []byte, fn func(key []byte, rows []Row) bool, pre *decodedNode) error {
	var nStack decodedNode
	var n *decodedNode
	if pre != nil {
		n = pre
	} else {
		if err := loadNodeInto(bm, nodeRef, &nStack); err != nil {
			return err
		}
		n = &nStack
	}
	start := len(buf)
	buf = append(buf, n.prefix...)
	nodeEnd := len(buf)
	if n.leaf != 0 {
		rows, err := decodeLeafRowsChain(bm, n.leaf)
		if err != nil {
			buf = buf[:start]
			return err
		}
		if fn(buf, rows) {
			buf = buf[:start]
			return errWalkStop
		}
	}
	for _, e := range n.edges {
		if err := recursiveWalkRef(bm, e.child, buf, fn, nil); err != nil {
			buf = buf[:start]
			return err
		}
		buf = buf[:nodeEnd]
	}
	buf = buf[:start]
	return nil
}

// Stats returns distinct key count and total row count (O(1) when header stats valid).
func (db *DB) Stats() (uint64, uint64, error) {
	if db.statsValid.Load() == 1 {
		return db.statDistinct.Load(), db.statTotal.Load(), nil
	}
	if db.readOnly {
		db.readCloseMu.RLock()
		defer db.readCloseMu.RUnlock()
		var distinct, total uint64
		err := db.walkPrefixBytesUnlocked(nil, func(_ []byte, rows []Row) bool {
			distinct++
			total += uint64(len(rows))
			return false
		})
		return distinct, total, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.statsValid.Load() == 1 {
		return db.statDistinct.Load(), db.statTotal.Load(), nil
	}
	var distinct, total uint64
	err := db.walkPrefixBytesUnlocked(nil, func(_ []byte, rows []Row) bool {
		distinct++
		total += uint64(len(rows))
		return false
	})
	if err != nil {
		return 0, 0, err
	}
	db.statDistinct.Store(distinct)
	db.statTotal.Store(total)
	db.statsValid.Store(1)
	db.flushHeader()
	return distinct, total, nil
}

// RootRef returns the current root disk reference (diagnostics).
func (db *DB) RootRef() Ref {
	return Ref(db.root.Load())
}

// Close releases resources after waiting for in-flight readers and writers.
func (db *DB) Close() error {
	db.mu.Lock()
	db.readCloseMu.Lock()
	defer func() {
		db.readCloseMu.Unlock()
		db.mu.Unlock()
	}()
	if db.f != nil {
		err := db.f.Close()
		db.f = nil
		return err
	}
	return nil
}

// Sync fsyncs the backing file.
func (db *DB) Sync() error {
	if db.f == nil {
		return nil
	}
	db.mu.Lock()
	db.flushHeader()
	db.mu.Unlock()
	return db.f.Sync()
}
