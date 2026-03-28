package radixdb

import (
	"time"
)

// Skip reasons for ShouldCompact / CompactIfNeeded (Prometheus labels, logs).
const (
	CompactSkipNone                 = ""
	CompactSkipFileTooSmall         = "file_too_small"
	CompactSkipNoReclaimable        = "no_reclaimable"
	CompactSkipReclaimBelowMin      = "reclaim_below_min"
	CompactSkipCooldown             = "cooldown"
	CompactSkipWasteBelowThresh     = "waste_below_threshold"
	CompactSkipAlreadyInflight      = "already_inflight"
	CompactSkipReadOnly             = "read_only"
	CompactSkipNoPath               = "no_path"
	CompactSkipNoWritesSinceCompact = "no_writes_since_compact"
)

// ShouldCompact reports whether compaction heuristics recommend a run. skipReason is
// one of the CompactSkip* constants when ok is false; reclaimable is an estimate of
// dead bytes (file size minus live reachable bytes); wastePct is reclaimable*100/fileSize.
func (db *DB) ShouldCompact() (ok bool, reclaimable uint64, wastePct uint64, skipReason string, err error) {
	db.readCloseMu.RLock()
	defer db.readCloseMu.RUnlock()
	return db.shouldCompactUnlocked()
}

func (db *DB) shouldCompactUnlocked() (ok bool, reclaimable uint64, wastePct uint64, skipReason string, err error) {
	if db.compactionLastRunUnixNano.Load() > 0 &&
		db.writeSeq.Load() == db.lastCompactWriteSeq.Load() {
		return false, 0, 0, CompactSkipNoWritesSinceCompact, nil
	}
	return db.shouldCompactWithLast(db.compactionLastRun())
}

func (db *DB) shouldCompactWithLast(last time.Time) (ok bool, reclaimable uint64, wastePct uint64, skipReason string, err error) {
	fileBytes := uint64(db.nBlocks.Load()) * uint64(BlockSize)
	if fileBytes < db.compactionMinFileBytes() {
		return false, 0, 0, CompactSkipFileTooSmall, nil
	}
	live, err := db.liveBytesUnlocked()
	if err != nil {
		return false, 0, 0, "", err
	}
	var reclaim uint64
	if fileBytes > live {
		reclaim = fileBytes - live
	}
	if reclaim == 0 {
		return false, 0, 0, CompactSkipNoReclaimable, nil
	}
	if reclaim < db.compactionMinReclaimBytes() {
		return false, reclaim, 0, CompactSkipReclaimBelowMin, nil
	}
	if !last.IsZero() && time.Since(last) < time.Duration(db.compactCooldownSec)*time.Second {
		return false, reclaim, 0, CompactSkipCooldown, nil
	}
	wastePct = reclaim * 100 / fileBytes
	if reclaim*100 < fileBytes*db.compactionWasteRatioPercent() {
		return false, reclaim, wastePct, CompactSkipWasteBelowThresh, nil
	}
	return true, reclaim, wastePct, CompactSkipNone, nil
}
