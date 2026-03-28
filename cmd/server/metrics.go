package main

import (
	"log"
	"net/http"
	"time"

	"artbenchmark/pkg/radixdb"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	distinctKeysGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "radixdb",
		Name:      "distinct_keys",
		Help:      "Number of distinct keys with at least one row (radixdb backend).",
	})
	totalRowsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "radixdb",
		Name:      "total_rows",
		Help:      "Total number of rows across all keys (radixdb backend).",
	})
	compactSkipTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "radixdb",
		Subsystem: "compact",
		Name:      "skip_total",
		Help:      "Compaction skipped by reason (matches radixdb.CompactSkip*).",
	}, []string{"reason"})
	compactDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "radixdb",
		Subsystem: "compact",
		Name:      "duration_seconds",
		Help:      "Duration of a CompactIfNeeded attempt that performed compaction.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 16),
	})
	compactRunsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "radixdb",
		Subsystem: "compact",
		Name:      "runs_total",
		Help:      "Compaction outcomes.",
	}, []string{"result"})
	compactInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "radixdb",
		Subsystem: "compact",
		Name:      "inflight",
		Help:      "1 while CompactIfNeeded is running on the DB.",
	})
)

func init() {
	prometheus.MustRegister(distinctKeysGauge, totalRowsGauge,
		compactSkipTotal, compactDuration, compactRunsTotal, compactInflight)
}

func metricsHandler() http.Handler {
	return promhttp.Handler()
}

func refreshMetricsFromDB(db *radixdb.DB) error {
	dk, tr, err := db.Stats()
	if err != nil {
		return err
	}
	distinctKeysGauge.Set(float64(dk))
	totalRowsGauge.Set(float64(tr))
	return nil
}

func bumpInsertMetrics(keyAlreadyHadRows bool) {
	if !keyAlreadyHadRows {
		distinctKeysGauge.Inc()
	}
	totalRowsGauge.Inc()
}

func recordCompactAttempt(db *radixdb.DB, ran bool, skipReason string, err error, d time.Duration) {
	if err != nil {
		compactRunsTotal.WithLabelValues("error").Inc()
		log.Printf("compact: error after %s: %v", d, err)
		return
	}
	if ran {
		compactRunsTotal.WithLabelValues("ok").Inc()
		compactDuration.Observe(d.Seconds())
		after := db.FileSizeBytes()
		log.Printf("compact: ok duration=%s bytes_after=%d", d, after)
		return
	}
	if skipReason != "" && skipReason != radixdb.CompactSkipNone {
		compactSkipTotal.WithLabelValues(skipReason).Inc()
	}
}
