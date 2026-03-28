package radixdb

import (
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	defaultCompactCooldown      = 10 * time.Second
	defaultCompactMinReclaimMB  = uint32(32)
	defaultCompactWasteRatioPct = uint32(50)
	defaultCompactMinFileMB     = uint32(8)
)

var (
	compactionDefaultsOnce sync.Once
	compactionDefaultCfg   struct {
		cooldown     time.Duration
		minReclaimMB uint32
	}
)

func defaultCompactionEnv() (cooldown time.Duration, minReclaimMB uint32) {
	compactionDefaultsOnce.Do(func() {
		compactionDefaultCfg.cooldown = defaultCompactCooldown
		compactionDefaultCfg.minReclaimMB = defaultCompactMinReclaimMB
		if s := os.Getenv("RDX2_COMPACT_COOLDOWN"); s != "" {
			if d, err := time.ParseDuration(s); err == nil && d >= 0 {
				compactionDefaultCfg.cooldown = d
			}
		}
		if s := os.Getenv("RDX2_COMPACT_MIN_RECLAIM_MB"); s != "" {
			if n, err := strconv.ParseUint(s, 10, 32); err == nil {
				compactionDefaultCfg.minReclaimMB = uint32(n)
			}
		}
	})
	return compactionDefaultCfg.cooldown, compactionDefaultCfg.minReclaimMB
}

func (db *DB) applyCompactionFromHeader(h FileHeader) {
	envCooldown, envMinMB := defaultCompactionEnv()
	sec := h.CompactCooldownSec
	if sec == 0 {
		sec = uint32(envCooldown / time.Second)
		if sec == 0 {
			sec = 1
		}
	}
	minMB := h.CompactMinReclaimMB
	if minMB == 0 {
		minMB = envMinMB
	}
	wastePct := h.CompactWasteRatioPct
	if wastePct == 0 {
		wastePct = defaultCompactWasteRatioPct
	}
	minFileMB := h.CompactMinFileMB
	if minFileMB == 0 {
		minFileMB = defaultCompactMinFileMB
	}
	db.compactCooldownSec = sec
	db.compactMinReclaimMB = minMB
	db.compactWasteRatioPct = wastePct
	db.compactMinFileMB = minFileMB
}

// SetCompactionConfig updates cooldown and minimum reclaim size persisted in the v2 header on next flush.
func (db *DB) SetCompactionConfig(cooldown time.Duration, minReclaimMB uint32) error {
	if db.readOnly {
		return ErrReadOnly
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if cooldown < 0 {
		cooldown = 0
	}
	sec := uint32(cooldown / time.Second)
	if cooldown > 0 && sec == 0 {
		sec = 1
	}
	db.compactCooldownSec = sec
	db.compactMinReclaimMB = minReclaimMB
	if db.compactWasteRatioPct == 0 {
		db.compactWasteRatioPct = defaultCompactWasteRatioPct
	}
	if db.compactMinFileMB == 0 {
		db.compactMinFileMB = defaultCompactMinFileMB
	}
	db.flushHeader()
	return nil
}

// SetCompactionHeuristics sets waste ratio (percent of file that must be reclaimable) and
// minimum file size (MB) before compaction heuristics apply. Zero values are replaced with defaults.
func (db *DB) SetCompactionHeuristics(wasteRatioPct, minFileMB uint32) error {
	if db.readOnly {
		return ErrReadOnly
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if wasteRatioPct == 0 {
		wasteRatioPct = defaultCompactWasteRatioPct
	}
	if minFileMB == 0 {
		minFileMB = defaultCompactMinFileMB
	}
	db.compactWasteRatioPct = wasteRatioPct
	db.compactMinFileMB = minFileMB
	db.flushHeader()
	return nil
}

// CompactionConfig returns the cooldown and minimum reclaim size (MB) stored for this database.
func (db *DB) CompactionConfig() (cooldown time.Duration, minReclaimMB uint32) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return time.Duration(db.compactCooldownSec) * time.Second, db.compactMinReclaimMB
}

func (db *DB) compactionMinReclaimBytes() uint64 {
	return uint64(db.compactMinReclaimMB) << 20
}

func (db *DB) compactionMinFileBytes() uint64 {
	return uint64(db.compactMinFileMB) << 20
}

func (db *DB) compactionWasteRatioPercent() uint64 {
	return uint64(db.compactWasteRatioPct)
}
