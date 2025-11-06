package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	MessagesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "messages_processed_total",
		Help: "Total number of messages successfully processed",
	})

	DLQCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dlq_count_total",
		Help: "Total number of messages pushed to DLQ",
	})

	DBLatency = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "db_latency_seconds",
		Help:       "DB query latency in seconds",
		Objectives: map[float64]float64{0.95: 0.01},
	})
)

func TrackDBLatency(start time.Time) {
	DBLatency.Observe(time.Since(start).Seconds())
}

func Init() error {
	// noop for now, ensures package is referenced
	return nil
}
