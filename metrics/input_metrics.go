package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	DroppedMetrics = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "input_dropped_metrics_total",
		Help: "The total number of metrics dropped at input",
	}, []string{"error"})
)
