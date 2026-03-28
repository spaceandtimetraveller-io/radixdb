package radixdb

import (
	"container/list"
	"os"
	"sync"
)

// DefaultBlockCacheEntries is the LRU capacity in 4 KiB blocks (not file size).
const DefaultBlockCacheEntries = 2048

// blockCacheShards splits the LRU so concurrent readBlock calls on different blocks
// rarely share a lock (mutex profile: readBlock dominated parallel walks).
// Total capacity remains DefaultBlockCacheEntries (sum of per-shard limits).
// Fewer/larger shards reduce single-thread thrash vs many small LRUs; 8×256 = 2048.
const blockCacheShards = 8

type lruEntry struct {
	id  uint32
	buf []byte
}

type blockShard struct {
	mu    sync.Mutex
	list  *list.List
	index map[uint32]*list.Element
}

// blockMgr is a buffer manager: fixed-size blocks via ReadAt/WriteAt, LRU-cached.
// A sync.Pool recycles block-sized buffers for loads; evicted or replaced cache buffers
// are not returned to the pool because callers may still reference slices from readBlock.
// Pins are modeled implicitly by readCloseMu at DB level for concurrent access.
type blockMgr struct {
	f           *os.File
	maxPerShard int
	shards      [blockCacheShards]blockShard
	pool        sync.Pool
}

func newBlockMgr(f *os.File) *blockMgr {
	m := &blockMgr{
		f: f,
		maxPerShard: (DefaultBlockCacheEntries + blockCacheShards - 1) / blockCacheShards,
	}
	for i := range m.shards {
		m.shards[i].list = list.New()
		m.shards[i].index = make(map[uint32]*list.Element)
	}
	m.pool.New = func() interface{} {
		return make([]byte, BlockSize)
	}
	return m
}

func (m *blockMgr) shard(id uint32) *blockShard {
	return &m.shards[id%blockCacheShards]
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

func (m *blockMgr) getBuf() []byte {
	b, _ := m.pool.Get().([]byte)
	if cap(b) < BlockSize {
		return make([]byte, BlockSize)
	}
	return b[:BlockSize]
}

// putBuf returns a block buffer to the pool only when it was never installed in the LRU
// (e.g. lost race on read). Never call for slices that were returned from readBlock.
func (m *blockMgr) putBuf(b []byte) {
	if cap(b) < BlockSize {
		return
	}
	m.pool.Put(b[:BlockSize])
}

func (m *blockMgr) evictShardLocked(s *blockShard) {
	el := s.list.Back()
	if el == nil {
		return
	}
	e := el.Value.(*lruEntry)
	s.list.Remove(el)
	delete(s.index, e.id)
}

// blockBufUnderLock returns the block buffer for id, loading from disk if needed.
// Caller must hold s.mu.
func (m *blockMgr) blockBufUnderLock(s *blockShard, id uint32) ([]byte, error) {
	if el, ok := s.index[id]; ok {
		s.list.MoveToFront(el)
		return el.Value.(*lruEntry).buf, nil
	}
	buf := m.getBuf()
	_, err := m.f.ReadAt(buf, int64(id)*int64(BlockSize))
	if err != nil {
		m.putBuf(buf)
		return nil, err
	}
	for len(s.index) >= m.maxPerShard {
		m.evictShardLocked(s)
	}
	e := &lruEntry{id: id, buf: buf}
	s.index[id] = s.list.PushFront(e)
	return buf, nil
}

// modifyBlock loads id under the shard lock, calls mut, then persists the block. Used for in-place
// bump allocation so eviction cannot steal the buffer between read and write.
func (m *blockMgr) modifyBlock(id uint32, mut func([]byte) error) error {
	s := m.shard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	buf, err := m.blockBufUnderLock(s, id)
	if err != nil {
		return err
	}
	if err := mut(buf); err != nil {
		return err
	}
	_, err = m.f.WriteAt(buf, int64(id)*int64(BlockSize))
	if err != nil {
		return err
	}
	if el, ok := s.index[id]; ok {
		s.list.MoveToFront(el)
	}
	return nil
}

// readBlock returns the cached block slice or reads it from disk. The returned slice must not be
// mutated by callers except via modifyBlock / internal alloc paths; it may alias the LRU cache.
func (m *blockMgr) readBlock(id uint32) ([]byte, error) {
	s := m.shard(id)
	s.mu.Lock()
	if el, ok := s.index[id]; ok {
		s.list.MoveToFront(el)
		buf := el.Value.(*lruEntry).buf
		s.mu.Unlock()
		return buf, nil
	}
	buf := m.getBuf()
	s.mu.Unlock()

	_, err := m.f.ReadAt(buf, int64(id)*int64(BlockSize))
	s.mu.Lock()
	if err != nil {
		m.putBuf(buf)
		s.mu.Unlock()
		return nil, err
	}
	if el, ok := s.index[id]; ok {
		m.putBuf(buf)
		s.list.MoveToFront(el)
		out := el.Value.(*lruEntry).buf
		s.mu.Unlock()
		return out, nil
	}
	for len(s.index) >= m.maxPerShard {
		m.evictShardLocked(s)
	}
	e := &lruEntry{id: id, buf: buf}
	s.index[id] = s.list.PushFront(e)
	s.mu.Unlock()
	return buf, nil
}

func (m *blockMgr) writeBlock(id uint32, data []byte) error {
	if len(data) != BlockSize {
		panic("radixdb: writeBlock size")
	}
	s := m.shard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := m.f.WriteAt(data, int64(id)*int64(BlockSize)); err != nil {
		return err
	}
	if el, ok := s.index[id]; ok {
		e := el.Value.(*lruEntry)
		copy(e.buf, data)
		s.list.MoveToFront(el)
		return nil
	}
	buf := m.getBuf()
	copy(buf, data)
	for len(s.index) >= m.maxPerShard {
		m.evictShardLocked(s)
	}
	e := &lruEntry{id: id, buf: buf}
	s.index[id] = s.list.PushFront(e)
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
