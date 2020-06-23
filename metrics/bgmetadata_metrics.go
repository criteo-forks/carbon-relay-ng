package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type BgMetadataMetrics struct {
	AddedMetrics          prometheus.Counter
	FilteredMetrics       prometheus.Counter
	BloomFilterEntries    *prometheus.CounterVec
	BloomFilterMaxEntries prometheus.Gauge
}

const bgMetadataNamespace = "metadata"

func NewBgMetadataMetrics(id string) BgMetadataMetrics {
	namespace := bgMetadataNamespace
	mm := BgMetadataMetrics{}
	labels := prometheus.Labels{
		"id": id,
	}

	mm.AddedMetrics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   namespace,
		Name:        "added_metrics_total",
		Help:        "total number of metrics added to metadata",
		ConstLabels: labels,
	})

	mm.FilteredMetrics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   namespace,
		Name:        "filtered_metrics_total",
		Help:        "total number of metrics filtered from adding to metadata",
		ConstLabels: labels,
	})

	mm.BloomFilterEntries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bloom_filter_entries",
		Help:      "number of entries in the bloom filter by shard",
	}, []string{"shard"})

	mm.BloomFilterMaxEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace:   namespace,
		Name:        "bloom_filter_max_entries",
		Help:        "max entries on all shards",
		ConstLabels: labels,
	})

	_ = prometheus.DefaultRegisterer.Register(mm.BloomFilterEntries)
	return mm
}
