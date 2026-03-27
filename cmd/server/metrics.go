package main

import (
	"net/http"

	"artbenchmark/radixdb"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	distinctKeysGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "radixdb",
		Name:      "distinct_keys",
		Help:      "Number of distinct keys with at least one row.",
	})
	totalRowsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "radixdb",
		Name:      "total_rows",
		Help:      "Total number of rows across all keys.",
	})
)

func init() {
	prometheus.MustRegister(distinctKeysGauge, totalRowsGauge)
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
