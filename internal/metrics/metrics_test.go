package metrics

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/limits"
)

func TestNormalizeStatusCode(t *testing.T) {
	if got := normalizeStatusCode(0); got != "unknown" {
		t.Fatalf("normalizeStatusCode(0) = %q, want %q", got, "unknown")
	}
	if got := normalizeStatusCode(204); got != "204" {
		t.Fatalf("normalizeStatusCode(204) = %q, want %q", got, "204")
	}
}

func TestClassifyVLogsResult(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{name: "success", err: nil, want: "success"},
		{name: "truncated", err: errors.New("vlogs response exceeded maximum allowed bytes"), want: "truncated"},
		{name: "timeout", err: errors.New("context deadline exceeded"), want: "timeout"},
		{name: "cancelled", err: errors.New("context canceled"), want: "timeout"},
		{name: "error", err: errors.New("boom"), want: "error"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyVLogsResult(tc.err); got != tc.want {
				t.Fatalf("classifyVLogsResult(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestMetricsHelpersRegisterAndGather(t *testing.T) {
	RegisterLimiter(limits.New(2, 1))
	IncHTTPInFlight("/loki/api/v1/query_range")
	DecHTTPInFlight("/loki/api/v1/query_range")
	ObserveHTTPRequest("GET", "/loki/api/v1/query_range", 200, 50*time.Millisecond, 1024)
	ObserveVLogs("query_logs", 20*time.Millisecond, nil)
	ObserveParseDuration(5 * time.Millisecond)
	ObserveTranslateDuration(5 * time.Millisecond)
	IncLimiterRejection("queue_full")
	IncCacheHit("field_names")
	IncCacheMiss("field_values")
	IncCacheSet("field_names")
	SetCacheEntries("field_values", 3)
	AddCacheEvictions("field_values", 1)
	AddCacheExpirations("field_names", 2)
	IncResponseTruncated("query_logs_body_limit")

	initRegistry()
	families, err := metricsReg.Gather()
	if err != nil {
		t.Fatalf("Gather(): %v", err)
	}

	names := make([]string, 0, len(families))
	for _, mf := range families {
		names = append(names, mf.GetName())
	}

	required := []string{
		"logql_proxy_http_requests_total",
		"logql_proxy_vlogs_requests_total",
		"logql_proxy_query_parse_duration_seconds",
		"logql_proxy_query_translate_duration_seconds",
		"logql_proxy_limiter_rejections_total",
		"logql_proxy_cache_hits_total",
		"logql_proxy_responses_truncated_total",
	}
	for _, name := range required {
		if !slices.Contains(names, name) {
			t.Fatalf("expected metric family %q to be registered", name)
		}
	}
}
