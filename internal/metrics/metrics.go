// Package metrics is the Prometheus instrumentation for the server plus the
// /metrics HTTP endpoint.
//
// Every metric registers on the default registry at package init (via promauto),
// so the recording helpers below are always safe to call and cost ~nothing when
// nobody scrapes. Only the HTTP endpoint is gated, behind --metrics-addr: a
// server with metrics disabled still records into these objects, it just never
// serves them. The live-state gauges (keys, memory, replication lag) are wired to
// the running server by Init, which is idempotent so tests can build many servers
// in one process without a double-registration panic.
package metrics

import (
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	commandsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mini_redis_commands_total",
		Help: "Commands processed, by command name and result (ok|error).",
	}, []string{"cmd", "result"})

	commandDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mini_redis_command_duration_seconds",
		Help:    "Command handling latency in seconds, by command name.",
		Buckets: prometheus.ExponentialBuckets(5e-6, 3, 12), // ~5µs .. ~0.3s
	}, []string{"cmd"})

	aofBytesWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mini_redis_aof_bytes_written_total",
		Help: "Total bytes appended to the AOF.",
	})

	aofFsyncDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "mini_redis_aof_fsync_duration_seconds",
		Help:    "AOF fsync latency in seconds.",
		Buckets: prometheus.ExponentialBuckets(1e-4, 3, 12), // ~100µs .. ~50s
	})

	connectionsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mini_redis_connections_active",
		Help: "Currently open client connections.",
	})
)

// enabled gates the recording helpers so a server started without --metrics-addr
// pays nothing on the hot path beyond one atomic load. Init flips it on.
var enabled atomic.Bool

// Recording helpers. Called from the hot paths; each is a single cheap op guarded
// by the enabled flag, so they are ~free when metrics are turned off.

// ObserveCommand records one processed command and its latency.
func ObserveCommand(cmd, result string, d time.Duration) {
	if !enabled.Load() {
		return
	}
	commandsTotal.WithLabelValues(cmd, result).Inc()
	commandDuration.WithLabelValues(cmd).Observe(d.Seconds())
}

// AddAOFBytes records n bytes flushed to the AOF.
func AddAOFBytes(n int) {
	if !enabled.Load() {
		return
	}
	aofBytesWritten.Add(float64(n))
}

// ObserveFsync records one AOF fsync's duration.
func ObserveFsync(d time.Duration) {
	if !enabled.Load() {
		return
	}
	aofFsyncDuration.Observe(d.Seconds())
}

// ConnOpened and ConnClosed track the live client connection count.
func ConnOpened() {
	if enabled.Load() {
		connectionsActive.Inc()
	}
}
func ConnClosed() {
	if enabled.Load() {
		connectionsActive.Dec()
	}
}

// Live-state providers, set by Init and read at scrape time.
var (
	registerOnce sync.Once
	keysFn       atomic.Value // func() float64
	lagFn        atomic.Value // func() map[string]float64
)

// Init wires the live-state gauges (keys, memory, per-replica replication lag) to
// the running server and registers them on first call. Later calls just refresh
// the providers, so building several servers in one process (tests) is safe.
func Init(keys func() float64, lag func() map[string]float64) {
	keysFn.Store(keys)
	lagFn.Store(lag)
	enabled.Store(true)
	registerOnce.Do(func() {
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "mini_redis_keys_total",
			Help: "Live keys across all shards (may briefly include lazily-expired keys).",
		}, func() float64 {
			if f, _ := keysFn.Load().(func() float64); f != nil {
				return f()
			}
			return 0
		})
		// ponytail: HeapAlloc approximates memory, not RSS; ReadMemStats briefly
		// stops the world, fine at a 15s scrape. Swap for an RSS reader if it matters.
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "mini_redis_memory_bytes",
			Help: "Go heap bytes in use (approximates process memory; not RSS).",
		}, func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return float64(m.HeapAlloc)
		})
		prometheus.MustRegister(lagCollector{desc: prometheus.NewDesc(
			"mini_redis_replication_lag_seconds",
			"Seconds since each connected replica last acked a heartbeat.",
			[]string{"replica"}, nil,
		)})
	})
}

// lagCollector emits one replication-lag series per connected replica at scrape
// time. A collector (not a GaugeVec) is used so replica label series appear and
// vanish with the connections themselves — no stale series to prune.
type lagCollector struct{ desc *prometheus.Desc }

func (c lagCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c lagCollector) Collect(ch chan<- prometheus.Metric) {
	f, _ := lagFn.Load().(func() map[string]float64)
	if f == nil {
		return
	}
	for replica, secs := range f() {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, secs, replica)
	}
}

// Handler is the /metrics HTTP handler the server mounts on --metrics-addr.
func Handler() http.Handler { return promhttp.Handler() }
