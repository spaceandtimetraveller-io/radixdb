package radixdb

import (
	"os"
	"time"
)

func (db *DB) compactionLastRun() time.Time {
	n := db.compactionLastRunUnixNano.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

func (db *DB) markCompactionRan() {
	db.compactionLastRunUnixNano.Store(time.Now().UnixNano())
}

// CompactIfNeeded runs offline-style compaction when heuristics say reclaim is worthwhile.
// It takes the writer mutex and an exclusive readCloseMu (blocks concurrent readers) for the
// duration, closes the backing file, rewrites via CompactFile, then reopens the path.
// Returns ran=true when a compaction was performed; skipReason is set when ran=false (see CompactSkip*).
func (db *DB) CompactIfNeeded() (ran bool, skipReason string, err error) {
	if db.readOnly {
		return false, CompactSkipReadOnly, nil
	}
	if db.path == "" {
		return false, CompactSkipNoPath, ErrNoPath
	}

	db.mu.Lock()
	if db.compactionInFlight {
		db.mu.Unlock()
		return false, CompactSkipAlreadyInflight, nil
	}
	db.readCloseMu.Lock()

	defer func() {
		db.readCloseMu.Unlock()
		db.mu.Unlock()
	}()

	ok, _, _, skip, ierr := db.shouldCompactUnlocked()
	if ierr != nil {
		return false, "", ierr
	}
	if !ok {
		return false, skip, nil
	}

	db.compactionInFlight = true
	defer func() { db.compactionInFlight = false }()

	if db.f != nil {
		if err := db.f.Close(); err != nil {
			return false, "", err
		}
		db.f = nil
	}

	path := db.path
	if err := CompactFile(path, path); err != nil {
		if rerr := db.reopenWriter(path); rerr != nil {
			return false, "", rerr
		}
		return false, "", err
	}

	if err := db.reopenWriter(path); err != nil {
		return false, "", err
	}
	db.markCompactionRan()
	db.lastCompactWriteSeq.Store(db.writeSeq.Load())
	db.flushHeader()
	return true, CompactSkipNone, nil
}

func (db *DB) reopenWriter(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	bm := newBlockMgr(f)
	hdr, err := bm.readBlock(0)
	if err != nil {
		_ = f.Close()
		return err
	}
	h, err := parseHeaderBlock(hdr)
	if err != nil {
		_ = f.Close()
		return err
	}
	nbFromFile, err := bm.blockCountFromFile()
	if err != nil {
		_ = f.Close()
		return err
	}
	if h.NBlocks != nbFromFile {
		_ = f.Close()
		return ErrCorrupt
	}
	db.f = f
	db.bm = bm
	return db.applyHeader(h)
}

func (db *DB) applyHeader(h FileHeader) error {
	db.root.Store(uint64(h.Root))
	db.nBlocks.Store(h.NBlocks)
	db.allocBlock = h.AllocBlock
	db.allocOff = h.AllocOff
	db.statDistinct.Store(h.Distinct)
	db.statTotal.Store(h.Total)
	if h.StatsValid {
		db.statsValid.Store(1)
	} else {
		db.statsValid.Store(0)
	}
	db.applyCompactionFromHeader(h)
	return nil
}

// FileSizeBytes returns the current on-disk size (nBlocks * BlockSize).
func (db *DB) FileSizeBytes() uint64 {
	return uint64(db.nBlocks.Load()) * uint64(BlockSize)
}
