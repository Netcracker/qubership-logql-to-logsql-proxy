package loki_test

import (
	"testing"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/config"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// ────────────────────────────────────────────────────────────────────────────
// StreamGrouper tests
// ────────────────────────────────────────────────────────────────────────────

func TestGroupSingleStream(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	_ = g.Add(vlogs.Record{
		"_time": "2024-01-15T12:00:00Z",
		"_msg":  "hello world",
		"app":   "api",
	})

	streams := g.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if streams[0].Stream["app"] != "api" {
		t.Errorf("stream.app = %q, want %q", streams[0].Stream["app"], "api")
	}
	if len(streams[0].Values) != 1 {
		t.Fatalf("expected 1 value, got %d", len(streams[0].Values))
	}
	if streams[0].Values[0][1] != "hello world" {
		t.Errorf("log line = %q, want %q", streams[0].Values[0][1], "hello world")
	}
}

func TestGroupMultipleStreams(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	records := []vlogs.Record{
		{"_time": "2024-01-15T12:00:00Z", "_msg": "line1", "app": "api"},
		{"_time": "2024-01-15T12:00:01Z", "_msg": "line2", "app": "worker"},
		{"_time": "2024-01-15T12:00:02Z", "_msg": "line3", "app": "api"},
	}
	for _, r := range records {
		_ = g.Add(r)
	}

	streams := g.Streams()
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(streams))
	}
	// Streams are sorted by key, so "api" < "worker" alphabetically.
	if streams[0].Stream["app"] != "api" {
		t.Errorf("streams[0].app = %q, want %q", streams[0].Stream["app"], "api")
	}
	if len(streams[0].Values) != 2 {
		t.Errorf("api stream: expected 2 values, got %d", len(streams[0].Values))
	}
	if streams[1].Stream["app"] != "worker" {
		t.Errorf("streams[1].app = %q, want %q", streams[1].Stream["app"], "worker")
	}
}

func TestGroupTimestampNano(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	_ = g.Add(vlogs.Record{
		"_time": "2024-01-15T12:00:00.123456789Z",
		"_msg":  "precise timestamp",
		"app":   "api",
	})

	streams := g.Streams()
	ts := streams[0].Values[0][0]

	// The timestamp should be a nanosecond Unix timestamp string.
	// 2024-01-15T12:00:00.123456789Z → should contain "123456789" at the end.
	if len(ts) < 10 {
		t.Errorf("timestamp too short: %q", ts)
	}
	// The last 9 digits should be 123456789.
	if len(ts) >= 9 && ts[len(ts)-9:] != "123456789" {
		t.Errorf("nanosecond part = %q, want %q", ts[len(ts)-9:], "123456789")
	}
}

func TestGroupMaxStreamsEnforced(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 2)

	for i := 0; i < 5; i++ {
		_ = g.Add(vlogs.Record{
			"_time": "2024-01-15T12:00:00Z",
			"_msg":  "msg",
			"svc":   string(rune('a' + i)), // distinct label value per record
		})
	}

	streams := g.Streams()
	if len(streams) != 2 {
		t.Errorf("expected max 2 streams, got %d", len(streams))
	}
	if !g.Truncated() {
		t.Error("expected Truncated() == true after cap exceeded")
	}
}

func TestGroupMaxStreamsNotTruncatedWhenUnderCap(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 10)
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "_msg": "m", "a": "1"})
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:01Z", "_msg": "m", "a": "1"})

	if g.Truncated() {
		t.Error("expected Truncated() == false")
	}
	if len(g.Streams()) != 1 {
		t.Errorf("expected 1 stream, got %d", len(g.Streams()))
	}
}

func TestGroupKnownLabelsFilter(t *testing.T) {
	// Only "app" is in the known-labels allowlist; "host" should be excluded
	// from the stream key.
	g := loki.NewStreamGrouper([]string{"app"}, 100)
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "_msg": "m", "app": "api", "host": "h1"})
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:01Z", "_msg": "m", "app": "api", "host": "h2"})

	// Both records have the same "app" value → they should be in the same stream.
	streams := g.Streams()
	if len(streams) != 1 {
		t.Errorf("expected 1 stream (host excluded from key), got %d", len(streams))
	}
	if _, ok := streams[0].Stream["host"]; ok {
		t.Error("stream should not contain 'host' label (not in known-labels)")
	}
}

func TestGroupValuesAreSortedByTimestamp(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	// Add records out-of-order; Streams() should return them sorted.
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:02Z", "_msg": "third", "app": "a"})
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "_msg": "first", "app": "a"})
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:01Z", "_msg": "second", "app": "a"})

	values := g.Streams()[0].Values
	if values[0][1] != "first" || values[1][1] != "second" || values[2][1] != "third" {
		t.Errorf("values not sorted: %v", values)
	}
}

func TestGroupEmptyMsgField(t *testing.T) {
	g := loki.NewStreamGrouper(nil, 100)
	// Record with no _msg: should still be added with an empty log line.
	_ = g.Add(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "app": "x"})
	streams := g.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if streams[0].Values[0][1] != "" {
		t.Errorf("empty _msg: got %q, want %q", streams[0].Values[0][1], "")
	}
}

func TestEnrichedStreamClassification(t *testing.T) {
	g := loki.NewEnrichedStreamGrouper(nil, loki.EnrichmentConfig{
		Labels: config.LabelsConfig{
			KnownLabels:             []string{"service_name", "detected_level"},
			KnownParsedFields:       []string{"parse_format", "parse_status"},
			KnownStructuredMetadata: []string{"labels.component"},
			ExcludedFields:          []string{"_stream", "_stream_id"},
			LabelRemap:              map[string]string{"detected_level": "level"},
		},
	}, 100)

	_ = g.Add(vlogs.Record{
		"_time":            "2024-01-15T12:00:00Z",
		"_msg":             "hello world",
		"container":        "api",
		"level":            "warn",
		"parse_format":     "klog",
		"parse_status":     "success",
		"labels.component": "apiserver",
		"hostname":         "node-1",
		"_stream":          `{container="api"}`,
	})

	streams := g.EnrichedStreams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 enriched stream, got %d", len(streams))
	}
	if len(streams[0].Entries) != 1 {
		t.Fatalf("expected 1 enriched entry, got %d", len(streams[0].Entries))
	}
	entry := streams[0].Entries[0]
	if entry.Line != "hello world" {
		t.Fatalf("Line = %q, want %q", entry.Line, "hello world")
	}
	if entry.IndexedLabels["service_name"] != "api" {
		t.Fatalf("IndexedLabels = %v, want service_name=api", entry.IndexedLabels)
	}
	if entry.IndexedLabels["detected_level"] != "warn" {
		t.Fatalf("IndexedLabels = %v, want remapped level field to expose detected_level", entry.IndexedLabels)
	}
	if entry.ParsedFields["parse_format"] != "klog" || entry.ParsedFields["parse_status"] != "success" {
		t.Fatalf("ParsedFields = %v", entry.ParsedFields)
	}
	if entry.StructuredMetadata["labels.component"] != "apiserver" {
		t.Fatalf("StructuredMetadata = %v", entry.StructuredMetadata)
	}
	if entry.OtherFields["hostname"] != "node-1" {
		t.Fatalf("OtherFields = %v", entry.OtherFields)
	}
	if _, ok := entry.OtherFields["_stream"]; ok {
		t.Fatalf("excluded field leaked into OtherFields: %v", entry.OtherFields)
	}
}

func TestCategorizedStreamsShapeUsesIndexedLabelsAndMetadataTuple(t *testing.T) {
	g := loki.NewEnrichedStreamGrouper(nil, loki.EnrichmentConfig{
		Labels: config.LabelsConfig{
			KnownLabels:             []string{"service_name", "detected_level", "namespace", "container"},
			KnownParsedFields:       []string{"parse_format", "parse_status"},
			KnownStructuredMetadata: []string{"labels.component"},
			ExcludedFields:          []string{"_stream", "_stream_id"},
			LabelRemap:              map[string]string{"detected_level": "level"},
		},
		UseIndexedLabelsAsStream:   true,
		UseStreamFieldAsBaseLabels: true,
	}, 100)

	_ = g.Add(vlogs.Record{
		"_time":            "2024-01-15T12:00:00Z",
		"_msg":             "hello world",
		"_stream":          `{container="api",namespace="prod"}`,
		"container":        "api",
		"namespace":        "prod",
		"nodename":         "node-a",
		"level":            "warn",
		"parse_format":     "klog",
		"labels.component": "apiserver",
		"hostname":         "node-1",
	})

	streams := g.CategorizedStreams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 categorized stream, got %d", len(streams))
	}
	stream := streams[0]
	if stream.Stream["service_name"] != "api" || stream.Stream["detected_level"] != "warn" {
		t.Fatalf("unexpected categorized stream labels: %v", stream.Stream)
	}
	if stream.Stream["container"] != "api" || stream.Stream["namespace"] != "prod" {
		t.Fatalf("expected _stream labels to be preserved, got %v", stream.Stream)
	}
	if _, ok := stream.Stream["hostname"]; ok {
		t.Fatalf("non-indexed field leaked into stream labels: %v", stream.Stream)
	}
	if _, ok := stream.Stream["nodename"]; ok {
		t.Fatalf("expected non-_stream known label nodename to stay out of indexed labels: %v", stream.Stream)
	}
	if len(stream.Values) != 1 || len(stream.Values[0]) != 3 {
		t.Fatalf("expected single 3-tuple, got %#v", stream.Values)
	}
	meta, ok := stream.Values[0][2].(map[string]map[string]string)
	if !ok {
		t.Fatalf("tuple metadata type = %T, want map[string]map[string]string", stream.Values[0][2])
	}
	if meta["parsed"]["parse_format"] != "klog" {
		t.Fatalf("parsed metadata = %v", meta["parsed"])
	}
	if meta["parsed"]["labels.component"] != "apiserver" ||
		meta["parsed"]["hostname"] != "node-1" {
		t.Fatalf("parsed metadata = %v", meta["parsed"])
	}
	if _, ok := meta["structuredMetadata"]; ok {
		t.Fatalf("structured metadata should be empty in categorized shaping: %v", meta["structuredMetadata"])
	}
}

func TestSyntheticServiceNamePrefersContainerOverGenericName(t *testing.T) {
	g := loki.NewEnrichedStreamGrouper(nil, loki.EnrichmentConfig{
		Labels: config.LabelsConfig{
			KnownLabels: []string{"service_name", "container", "namespace"},
		},
		UseIndexedLabelsAsStream:   true,
		UseStreamFieldAsBaseLabels: true,
	}, 100)

	_ = g.Add(vlogs.Record{
		"_time":     "2024-01-15T12:00:00Z",
		"_msg":      "hello world",
		"_stream":   `{container="vlsingle-k8s",namespace="monitoring"}`,
		"container": "vlsingle-k8s",
		"namespace": "monitoring",
		"name":      "k8s",
	})

	streams := g.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if got := streams[0].Stream["service_name"]; got != "vlsingle-k8s" {
		t.Fatalf("service_name = %q, want %q", got, "vlsingle-k8s")
	}
}

func TestDetectedLevelPrefersExplicitDetectedLevelOverRemappedLevel(t *testing.T) {
	g := loki.NewEnrichedStreamGrouper(nil, loki.EnrichmentConfig{
		Labels: config.LabelsConfig{
			KnownLabels: []string{"service_name", "detected_level", "container", "namespace"},
			LabelRemap:  map[string]string{"detected_level": "level"},
		},
		UseIndexedLabelsAsStream:   true,
		UseStreamFieldAsBaseLabels: true,
	}, 100)

	_ = g.Add(vlogs.Record{
		"_time":          "2024-01-15T12:00:00Z",
		"_msg":           "hello world",
		"_stream":        `{container="cloud-provider-kind",namespace="kube-system"}`,
		"container":      "cloud-provider-kind",
		"namespace":      "kube-system",
		"level":          "info",
		"detected_level": "info",
		"source_level":   "I",
	})

	streams := g.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if got := streams[0].Stream["detected_level"]; got != "info" {
		t.Fatalf("detected_level = %q, want %q", got, "info")
	}
	if got := streams[0].Stream["level"]; got != "info" {
		t.Fatalf("level = %q, want %q", got, "info")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// ShapeMatrix tests
// ────────────────────────────────────────────────────────────────────────────

func TestMatrixResponseFormat(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	buckets := []vlogs.HitBucket{
		{Timestamp: now, Count: 42},
		{Timestamp: now.Add(time.Minute), Count: 58},
	}
	metric := map[string]string{"app": "api"}

	series := loki.ShapeMatrix(buckets, metric, false, 60)
	if len(series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(series))
	}
	s := series[0]
	if s.Metric["app"] != "api" {
		t.Errorf("metric.app = %q, want %q", s.Metric["app"], "api")
	}
	if len(s.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(s.Values))
	}
	// First value: [unix_seconds_float, "42"]
	ts0, ok := s.Values[0][0].(float64)
	if !ok {
		t.Fatalf("Values[0][0] is %T, want float64", s.Values[0][0])
	}
	if int64(ts0) != now.Unix() {
		t.Errorf("ts = %v, want %v", int64(ts0), now.Unix())
	}
	val0, ok := s.Values[0][1].(string)
	if !ok {
		t.Fatalf("Values[0][1] is %T, want string", s.Values[0][1])
	}
	if val0 != "42" {
		t.Errorf("value = %q, want %q", val0, "42")
	}
}

func TestMatrixRateQuery(t *testing.T) {
	now := time.Now().UTC()
	buckets := []vlogs.HitBucket{
		{Timestamp: now, Count: 120}, // 120 hits in 60s = 2/s rate
	}

	series := loki.ShapeMatrix(buckets, nil, true, 60)
	val := series[0].Values[0][1].(string)
	if val != "2" {
		t.Errorf("rate value = %q, want %q", val, "2")
	}
}

func TestMatrixNilMetric(t *testing.T) {
	buckets := []vlogs.HitBucket{{Timestamp: time.Now(), Count: 1}}
	series := loki.ShapeMatrix(buckets, nil, false, 60)
	if series[0].Metric == nil {
		t.Error("metric map should not be nil")
	}
}

func TestMatrixEmptyBuckets(t *testing.T) {
	series := loki.ShapeMatrix(nil, map[string]string{}, false, 60)
	if len(series) != 1 {
		t.Fatalf("expected 1 series even with empty buckets, got %d", len(series))
	}
	if len(series[0].Values) != 0 {
		t.Errorf("expected 0 values, got %d", len(series[0].Values))
	}
}

func TestInvalidTimestampFallsBackToZero(t *testing.T) {
	g := loki.NewStreamGrouper([]string{"app"}, 10)
	_ = g.Add(vlogs.Record{
		"_time": "not-a-time",
		"_msg":  "hello",
		"app":   "api",
	})

	streams := g.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if got := streams[0].Values[0][0]; got != "0" {
		t.Fatalf("timestamp = %q, want %q", got, "0")
	}
}
