package radixdb

import (
	"os"
	"sync"
)

// blockMgr is a minimal buffer manager: fixed-size blocks read/written via the file.
// Cached blocks are copied on read; writes go to file and update cache.
// Pins are modeled implicitly by readCloseMu at DB level for concurrent access.
type blockMgr struct {
	mu    sync.RWMutex
	f     *os.File
	cache map[uint32][]byte // full BlockSize each
}

func newBlockMgr(f *os.File) *blockMgr {
	return &blockMgr{f: f, cache: make(map[uint32][]byte)}
}

func (m *blockMgr) blockCountFromFile() (uint32, error) {
	st, err := m.f.Stat()
	if err != nil {
		return 0, err
	}
	n := st.Size() / int64(BlockSize)
	if st.Size()%int64(BlockSize) != 0 {
		return 0, ErrCorrupt
	}
	return uint32(n), nil
}

// readBlock returns the cached block slice or reads it from disk. The returned slice must not be
// mutated by callers; it may alias the in-memory cache (same as mmap-backed reads).
func (m *blockMgr) readBlock(id uint32) ([]byte, error) {
	m.mu.RLock()
	if b, ok := m.cache[id]; ok {
		m.mu.RUnlock()
		return b, nil
	}
	m.mu.RUnlock()

	buf := make([]byte, BlockSize)
	_, err := m.f.ReadAt(buf, int64(id)*int64(BlockSize))
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if b, ok := m.cache[id]; ok {
		m.mu.Unlock()
		return b, nil
	}
	m.cache[id] = buf
	m.mu.Unlock()
	return buf, nil
}

func (m *blockMgr) writeBlock(id uint32, data []byte) error {
	if len(data) != BlockSize {
		panic("radixdb: writeBlock size")
	}
	if _, err := m.f.WriteAt(data, int64(id)*int64(BlockSize)); err != nil {
		return err
	}
	m.mu.Lock()
	m.cache[id] = append([]byte(nil), data...)
	m.mu.Unlock()
	return nil
}

func (m *blockMgr) ensureNBlocks(n uint32) error {
	st, err := m.f.Stat()
	if err != nil {
		return err
	}
	need := int64(n) * int64(BlockSize)
	if st.Size() >= need {
		return nil
	}
	if err := m.f.Truncate(need); err != nil {
		return err
	}
	// zero-fill new region
	off := st.Size()
	for off < need {
		chunk := make([]byte, min64(int64(BlockSize), need-off))
		if _, err := m.f.WriteAt(chunk, off); err != nil {
			return err
		}
		off += int64(len(chunk))
	}
	return nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
