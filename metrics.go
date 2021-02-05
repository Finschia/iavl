package iavl

import (
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

const (
	// MetricsSubsystem is a subsystem shared by all metrics exposed by this
	// package.
	MetricsSubsystem = "iavl"
)

// Metrics contains metrics exposed by this package.
type Metrics struct {
	NodeCacheHits   metrics.Counter
	NodeCacheMisses metrics.Counter
}

// PrometheusMetrics returns Metrics build using Prometheus client library.
// Optionally, labels can be provided along with their values ("foo",
// "fooValue").
func PrometheusMetrics(namespace string, storeName string, labelsAndValues ...string) *Metrics {
	labels := []string{}
	for i := 0; i < len(labelsAndValues); i += 2 {
		labels = append(labels, labelsAndValues[i])
	}
	return &Metrics{
		NodeCacheHits: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      storeName + "_node_cache_hits",
			Help:      "Cache hits of the iavl node db cache",
		}, labels).With(labelsAndValues...),
		NodeCacheMisses: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      storeName + "_node_cache_misses",
			Help:      "Cache misses of the iavl node db cache",
		}, labels).With(labelsAndValues...),
	}
}

// NopMetrics returns no-op Metrics.
func NopMetrics() *Metrics {
	return &Metrics{
		NodeCacheHits:   discard.NewCounter(),
		NodeCacheMisses: discard.NewCounter(),
	}
}

type MetricsProvider func(storeName string) *Metrics

// PrometheusMetricsProvider returns PrometheusMetrics for each store
func PrometheusMetricsProvider(namespace string, labelsAndValues ...string) func(storeName string) *Metrics {
	return func(storeName string) *Metrics {
		return PrometheusMetrics(namespace, storeName, labelsAndValues...)
	}
}

// NopMetricsProvider returns NopMetrics for each store
func NopMetricsProvider() func(storeName string) *Metrics {
	return func(storeName string) *Metrics {
		return NopMetrics()
	}
}
