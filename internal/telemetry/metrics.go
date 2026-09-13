package telemetry

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// MetricType represents Prometheus metric types.
type MetricType string

const (
	MetricTypeCounter MetricType = "counter"
	MetricTypeGauge   MetricType = "gauge"
)

// MetricsRegistry provides a thread-safe in-memory store for operational metrics
// with native Prometheus text exposition (zero external dependencies).
type MetricsRegistry struct {
	mu       sync.RWMutex
	counters map[string]*uint64
	gauges   map[string]*int64
	helps    map[string]string
	types    map[string]MetricType
}

// Global registry instance.
var DefaultRegistry = NewMetricsRegistry()

// NewMetricsRegistry initializes a new MetricsRegistry.
func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{
		counters: make(map[string]*uint64),
		gauges:   make(map[string]*int64),
		helps:    make(map[string]string),
		types:    make(map[string]MetricType),
	}
}

func formatMetricName(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var labelPairs []string
	for _, k := range keys {
		labelPairs = append(labelPairs, fmt.Sprintf("%s=%q", k, labels[k]))
	}
	return fmt.Sprintf("%s{%s}", name, strings.Join(labelPairs, ","))
}

// RegisterCounter registers a counter with a help description.
func (r *MetricsRegistry) RegisterCounter(name, help string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.helps[name] = help
	r.types[name] = MetricTypeCounter
}

// RegisterGauge registers a gauge with a help description.
func (r *MetricsRegistry) RegisterGauge(name, help string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.helps[name] = help
	r.types[name] = MetricTypeGauge
}

// IncCounter increments a counter by 1.
func (r *MetricsRegistry) IncCounter(name string, labels map[string]string) {
	r.AddCounter(name, labels, 1)
}

// AddCounter adds delta to a counter.
func (r *MetricsRegistry) AddCounter(name string, labels map[string]string, delta uint64) {
	fullName := formatMetricName(name, labels)
	r.mu.Lock()
	ptr, exists := r.counters[fullName]
	if !exists {
		var val uint64
		r.counters[fullName] = &val
		ptr = &val
	}
	r.mu.Unlock()
	atomic.AddUint64(ptr, delta)
}

// SetGauge sets an instantaneous gauge value.
func (r *MetricsRegistry) SetGauge(name string, labels map[string]string, val int64) {
	fullName := formatMetricName(name, labels)
	r.mu.Lock()
	ptr, exists := r.gauges[fullName]
	if !exists {
		var v int64
		r.gauges[fullName] = &v
		ptr = &v
	}
	r.mu.Unlock()
	atomic.StoreInt64(ptr, val)
}

// AddGauge adjusts a gauge value by delta.
func (r *MetricsRegistry) AddGauge(name string, labels map[string]string, delta int64) {
	fullName := formatMetricName(name, labels)
	r.mu.Lock()
	ptr, exists := r.gauges[fullName]
	if !exists {
		var v int64
		r.gauges[fullName] = &v
		ptr = &v
	}
	r.mu.Unlock()
	atomic.AddInt64(ptr, delta)
}

// RenderPrometheusText generates the Prometheus text exposition output.
func (r *MetricsRegistry) RenderPrometheusText() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var sb strings.Builder

	// Sort base metric names for deterministic output
	baseNames := make([]string, 0, len(r.helps))
	for name := range r.helps {
		baseNames = append(baseNames, name)
	}
	sort.Strings(baseNames)

	for _, baseName := range baseNames {
		help := r.helps[baseName]
		mType := r.types[baseName]

		sb.WriteString(fmt.Sprintf("# HELP %s %s\n", baseName, help))
		sb.WriteString(fmt.Sprintf("# TYPE %s %s\n", baseName, mType))

		// Render matching metric lines
		switch mType {
		case MetricTypeCounter:
			for key, val := range r.counters {
				if key == baseName || strings.HasPrefix(key, baseName+"{") {
					v := atomic.LoadUint64(val)
					sb.WriteString(fmt.Sprintf("%s %d\n", key, v))
				}
			}
		case MetricTypeGauge:
			for key, val := range r.gauges {
				if key == baseName || strings.HasPrefix(key, baseName+"{") {
					v := atomic.LoadInt64(val)
					sb.WriteString(fmt.Sprintf("%s %d\n", key, v))
				}
			}
		}
	}

	return sb.String()
}

// Handler returns an http.HandlerFunc exposing the Prometheus /metrics endpoint.
func (r *MetricsRegistry) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.RenderPrometheusText()))
	}
}

// Standard metric names initialized in DefaultRegistry.
func init() {
	DefaultRegistry.RegisterCounter("forgeflow_jobs_submitted_total", "Total count of submitted jobs")
	DefaultRegistry.RegisterCounter("forgeflow_jobs_completed_total", "Total count of completed jobs")
	DefaultRegistry.RegisterCounter("forgeflow_jobs_failed_total", "Total count of failed jobs")
	DefaultRegistry.RegisterCounter("forgeflow_leases_lost_total", "Total count of rejected stale worker mutations due to lease expiration")
	DefaultRegistry.RegisterCounter("forgeflow_leases_reaped_total", "Total count of expired job leases recovered by the reaper")
	DefaultRegistry.RegisterCounter("forgeflow_workflows_completed_total", "Total count of completed workflows")
	DefaultRegistry.RegisterCounter("forgeflow_workflows_failed_total", "Total count of failed workflows")
	DefaultRegistry.RegisterGauge("forgeflow_running_jobs", "Current number of concurrently executing jobs")
	DefaultRegistry.RegisterGauge("forgeflow_active_workers", "Current number of active worker slots")
}
