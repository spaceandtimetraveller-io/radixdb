package main

import (
	"artbenchmark/pkg/radixdb"
	"context"
	"time"
)

func runPeriodicCompaction(ctx context.Context, db *radixdb.DB, every time.Duration) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			start := time.Now()
			compactInflight.Set(1)
			ran, reason, err := db.CompactIfNeeded()
			compactInflight.Set(0)
			recordCompactAttempt(db, ran, reason, err, time.Since(start))
			_ = refreshMetricsFromDB(db)
		}
	}
}
