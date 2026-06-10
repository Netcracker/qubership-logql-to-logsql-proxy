package metrics

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/limits"
)

var (
	registryOnce sync.Once
	metricsReg   *prometheus.Registry

	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logql_proxy_http_requests_total",
			Help: "Total number of HTTP requests handled by the proxy.",
		},
		[]string{"method", "route", "status_code"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logql_proxy_http_request_duration_seconds",
			Help:    "End-to-end HTTP request latency.",
			Buckets: durationBuckets,
		},
		[]string{"method", "route"},
	)
	httpInFlight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "logql_proxy_http_in_flight_requests",
			Help: "Current number of in-flight HTTP requests.",
		},
		[]string{"route"},
	)
	httpResponseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logql_proxy_http_response_size_bytes",
			Help:    "HTTP response size in bytes.",
			Buckets: responseSizeBuckets,
		},
		[]string{"method", "route"},
	)

	vlogsRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logql_proxy_vlogs_requests_total",
			Help: "Total number of outbound VictoriaLogs requests.",
		},
		[]string{"operation", "result"},
	)
	vlogsRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "logql_proxy_vlogs_request_duration_seconds",
			Help:    "Latency of outbound VictoriaLogs requests.",
			Buckets: durationBuckets,
		},
		[]string{"operation"},
	)

	queryParseDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "logql_proxy_query_parse_duration_seconds",
			Help:    "Time spent parsing LogQL queries.",
			Buckets: durationBuckets,
		},
	)
	queryTranslateDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "logql_proxy_query_translate_duration_seconds",
			Help:    "Time spent translating LogQL into LogsQL.",
			Buckets: durationBuckets,
		},
	)

	limiterActiveRequests = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "logql_proxy_limiter_active_requests",
			Help: "Current number of active requests accounted by the concurrency limiter.",
		},
		func() float64 {
			limiterMu.RLock()
			defer limiterMu.RUnlock()
			if currentLimiter == nil {
				return 0
			}
			return float64(currentLimiter.ActiveCount())
		},
	)
	limiterQueuedRequests = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "logql_proxy_limiter_queued_requests",
			Help: "Current number of queued requests waiting on the concurrency limiter.",
		},
		func() float64 {
			limiterMu.RLock()
			defer limiterMu.RUnlock()
			if currentLimiter == nil {
				return 0
			}
			return float64(currentLimiter.QueuedCount())
		},
	)
	limiterRejectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logql_proxy_limiter_rejections_total",
			Help: "Total number of requests rejected by the concurrency limiter.",
		},
		[]string{"reason"},
	)

	cacheHitsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logql_proxy_cache_hits_total",
			Help: "Total number of metadata cache hits.",
		},
		[]string{"cache"},
	)
	cacheMissesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logql_proxy_cache_misses_total",
			Help: "Total number of metadata cache misses.",
		},
		[]string{"cache"},
	)
	cacheSetsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logql_proxy_cache_sets_total",
			Help: "Total number of metadata cache insertions or updates.",
		},
		[]string{"cache"},
	)
	cacheEntries = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "logql_proxy_cache_entries",
			Help: "Current number of entries stored in each metadata cache.",
		},
		[]string{"cache"},
	)
	cacheEvictionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logql_proxy_cache_evictions_total",
			Help: "Total number of live cache entries evicted due to capacity pressure.",
		},
		[]string{"cache"},
	)
	cacheExpirationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logql_proxy_cache_expirations_total",
			Help: "Total number of cache entries removed because their TTL expired.",
		},
		[]string{"cache"},
	)

	responsesTruncatedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logql_proxy_responses_truncated_total",
			Help: "Total number of truncated responses returned by the proxy.",
		},
		[]string{"reason"},
	)

	limiterMu      sync.RWMutex
	currentLimiter *limits.Limiter
)

var (
	durationBuckets     = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	responseSizeBuckets = []float64{512, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216}
)

func initRegistry() {
	registryOnce.Do(func() {
		metricsReg = prometheus.NewRegistry()
		metricsReg.MustRegister(
			httpRequestsTotal,
			httpRequestDuration,
			httpInFlight,
			httpResponseSize,
			vlogsRequestsTotal,
			vlogsRequestDuration,
			queryParseDuration,
			queryTranslateDuration,
			limiterActiveRequests,
			limiterQueuedRequests,
			limiterRejectionsTotal,
			cacheHitsTotal,
			cacheMissesTotal,
			cacheSetsTotal,
			cacheEntries,
			cacheEvictionsTotal,
			cacheExpirationsTotal,
			responsesTruncatedTotal,
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
	})
}

func Handler() fasthttp.RequestHandler {
	initRegistry()
	handler := promhttp.InstrumentMetricHandler(
		metricsReg,
		promhttp.HandlerFor(metricsReg, promhttp.HandlerOpts{}),
	)
	return fasthttpadaptor.NewFastHTTPHandler(handler)
}

func RegisterLimiter(lim *limits.Limiter) {
	initRegistry()
	limiterMu.Lock()
	currentLimiter = lim
	limiterMu.Unlock()
}

func IncHTTPInFlight(route string) {
	initRegistry()
	httpInFlight.WithLabelValues(route).Inc()
}

func DecHTTPInFlight(route string) {
	initRegistry()
	httpInFlight.WithLabelValues(route).Dec()
}

func ObserveHTTPRequest(method, route string, statusCode int, duration time.Duration, responseBytes int) {
	initRegistry()
	httpRequestsTotal.WithLabelValues(method, route, normalizeStatusCode(statusCode)).Inc()
	httpRequestDuration.WithLabelValues(method, route).Observe(duration.Seconds())
	if responseBytes >= 0 {
		httpResponseSize.WithLabelValues(method, route).Observe(float64(responseBytes))
	}
}

func ObserveVLogs(operation string, duration time.Duration, err error) {
	initRegistry()
	vlogsRequestsTotal.WithLabelValues(operation, classifyVLogsResult(err)).Inc()
	vlogsRequestDuration.WithLabelValues(operation).Observe(duration.Seconds())
}

func ObserveParseDuration(duration time.Duration) {
	initRegistry()
	queryParseDuration.Observe(duration.Seconds())
}

func ObserveTranslateDuration(duration time.Duration) {
	initRegistry()
	queryTranslateDuration.Observe(duration.Seconds())
}

func IncLimiterRejection(reason string) {
	initRegistry()
	limiterRejectionsTotal.WithLabelValues(reason).Inc()
}

func IncCacheHit(cache string) {
	initRegistry()
	cacheHitsTotal.WithLabelValues(cache).Inc()
}

func IncCacheMiss(cache string) {
	initRegistry()
	cacheMissesTotal.WithLabelValues(cache).Inc()
}

func IncCacheSet(cache string) {
	initRegistry()
	cacheSetsTotal.WithLabelValues(cache).Inc()
}

func SetCacheEntries(cache string, entries int) {
	initRegistry()
	cacheEntries.WithLabelValues(cache).Set(float64(entries))
}

func AddCacheEvictions(cache string, n int) {
	initRegistry()
	if n > 0 {
		cacheEvictionsTotal.WithLabelValues(cache).Add(float64(n))
	}
}

func AddCacheExpirations(cache string, n int) {
	initRegistry()
	if n > 0 {
		cacheExpirationsTotal.WithLabelValues(cache).Add(float64(n))
	}
}

func IncResponseTruncated(reason string) {
	initRegistry()
	responsesTruncatedTotal.WithLabelValues(reason).Inc()
}

func normalizeStatusCode(statusCode int) string {
	if statusCode <= 0 {
		return "unknown"
	}
	return strconv.Itoa(statusCode)
}

func classifyVLogsResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case strings.Contains(err.Error(), "maximum allowed bytes"),
		strings.Contains(err.Error(), "response exceeded maximum allowed bytes"):
		return "truncated"
	case strings.Contains(err.Error(), "context deadline exceeded"),
		strings.Contains(err.Error(), "context canceled"):
		return "timeout"
	default:
		return "error"
	}
}
