package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/config"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/fieldclass"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/limits"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

type stubVL struct {
	queryLogsFn   func(context.Context, vlogs.LogQueryRequest, func(vlogs.Record) error) error
	queryHitsFn   func(context.Context, vlogs.HitsQueryRequest) ([]vlogs.HitBucket, error)
	fieldNamesFn  func(context.Context, vlogs.FieldNamesRequest) ([]string, error)
	fieldValuesFn func(context.Context, vlogs.FieldValuesRequest) ([]string, error)
}

func (s *stubVL) QueryLogs(ctx context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
	if s.queryLogsFn != nil {
		return s.queryLogsFn(ctx, req, fn)
	}
	return nil
}

func (s *stubVL) QueryHits(ctx context.Context, req vlogs.HitsQueryRequest) ([]vlogs.HitBucket, error) {
	if s.queryHitsFn != nil {
		return s.queryHitsFn(ctx, req)
	}
	return nil, nil
}

func (s *stubVL) FieldNames(ctx context.Context, req vlogs.FieldNamesRequest) ([]string, error) {
	if s.fieldNamesFn != nil {
		return s.fieldNamesFn(ctx, req)
	}
	return nil, nil
}

func (s *stubVL) FieldValues(ctx context.Context, req vlogs.FieldValuesRequest) ([]string, error) {
	if s.fieldValuesFn != nil {
		return s.fieldValuesFn(ctx, req)
	}
	return nil, nil
}

func testDeps(vl vlogs.VLogsClient) *Deps {
	cfg := &config.Config{}
	cfg.VLogs.URL = "http://victorialogs:9428"
	cfg.VLogs.Timeout = 5 * time.Second
	cfg.Limits.MaxConcurrentQueries = 2
	cfg.Limits.MaxQueueDepth = 1
	cfg.Limits.MaxResponseBodyBytes = 1 << 20
	cfg.Limits.MaxStreamsPerResponse = 100
	cfg.Limits.MaxQueryRangeHours = 24
	cfg.Limits.MaxLimit = 100
	cfg.Limits.DefaultLimit = 10
	cfg.Labels.KnownParsedFields = []string{"parse_format", "parse_status", "file", "source_level", "klog_date", "date", "pid"}
	cfg.Labels.KnownStructuredMetadata = []string{"labels.component", "labels.tier", "log_category"}
	cfg.Labels.ExcludedFields = []string{"_msg", "_time", "_stream", "_stream_id"}
	cfg.Labels.MetadataCacheTTL = time.Minute
	cfg.Labels.MetadataCacheSize = 16
	cfg.Labels.LabelRemap = map[string]string{"detected_level": "level"}
	return &Deps{
		Cfg:   cfg,
		VL:    vl,
		Lim:   limits.New(2, 1),
		Cache: vlogs.NewMetadataCache(16),
	}
}

func newCtx(target string) *fasthttp.RequestCtx {
	req := fasthttp.AcquireRequest()
	req.Header.SetMethod(http.MethodGet)
	req.SetRequestURI(target)
	var ctx fasthttp.RequestCtx
	ctx.Init(req, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}, nil)
	return &ctx
}

func decodeBody[T any](t *testing.T, ctx *fasthttp.RequestCtx) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(ctx.Response.Body(), &out); err != nil {
		t.Fatalf("json.Unmarshal(): %v; body=%s", err, ctx.Response.Body())
	}
	return out
}

func TestQueryInstantUsesOneSecondWindow(t *testing.T) {
	var got vlogs.LogQueryRequest
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			got = req
			return fn(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "_msg": "ok", "app": "api"})
		},
	})

	ctx := newCtx(`/loki/api/v1/query?query={app="api"}&time=1705320000`)
	deps.Query(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200", ctx.Response.StatusCode())
	}
	if got.End.Sub(got.Start) != time.Second {
		t.Errorf("instant query window = %v, want 1s", got.End.Sub(got.Start))
	}
	if got.Limit != deps.Cfg.Limits.DefaultLimit {
		t.Errorf("limit = %d, want %d", got.Limit, deps.Cfg.Limits.DefaultLimit)
	}

	body := decodeBody[loki.StreamsResponse](t, ctx)
	if body.Status != "success" || body.Data.ResultType != "streams" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestQueryRangeEmptyContainsFilterIsNoop(t *testing.T) {
	var got vlogs.LogQueryRequest
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			got = req
			return fn(vlogs.Record{
				"_time":     "2024-01-15T12:00:00Z",
				"_msg":      "log line",
				"container": "kindnet-cni",
			})
		},
	})

	ctx := newCtx("/loki/api/v1/query_range?query=%7Bcontainer%3D%22kindnet-cni%22%7D+%7C%3D+%60%60&start=1705320000&end=1705323600")
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if got.Query != `container:="kindnet-cni"` {
		t.Fatalf("translated query = %q, want %q", got.Query, `container:="kindnet-cni"`)
	}

	body := decodeBody[loki.StreamsResponse](t, ctx)
	if len(body.Data.Result) != 1 {
		t.Fatalf("stream count = %d, want 1", len(body.Data.Result))
	}
	if len(body.Data.Result[0].Values) != 1 {
		t.Fatalf("value count = %d, want 1", len(body.Data.Result[0].Values))
	}
}

func TestQueryRangeCategorizedLabelsResponseUsesThreeTupleMetadata(t *testing.T) {
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			return fn(vlogs.Record{
				"_time":            "2024-01-15T12:00:00Z",
				"_msg":             "hello world",
				"container":        "api",
				"namespace":        "prod",
				"level":            "warn",
				"parse_format":     "klog",
				"labels.component": "apiserver",
				"hostname":         "node-1",
			})
		},
	})
	deps.Cfg.Labels.KnownLabels = []string{"service_name", "detected_level", "namespace", "container"}

	ctx := newCtx(`/loki/api/v1/query_range?query={container="api"}&start=1705320000&end=1705323600`)
	ctx.Request.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	var body struct {
		Status string `json:"status"`
		Data   struct {
			ResultType    string   `json:"resultType"`
			EncodingFlags []string `json:"encodingFlags"`
			Result        []struct {
				Stream map[string]string `json:"stream"`
				Values []json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(ctx.Response.Body(), &body); err != nil {
		t.Fatalf("json.Unmarshal(): %v body=%s", err, ctx.Response.Body())
	}
	if body.Status != "success" || body.Data.ResultType != "streams" {
		t.Fatalf("unexpected categorized response: %+v", body)
	}
	if !slices.Contains(body.Data.EncodingFlags, "categorize-labels") {
		t.Fatalf("encodingFlags = %v, want categorize-labels", body.Data.EncodingFlags)
	}
	if len(body.Data.Result) != 1 {
		t.Fatalf("expected 1 result stream, got %d", len(body.Data.Result))
	}
	stream := body.Data.Result[0]
	if stream.Stream["service_name"] != "api" || stream.Stream["detected_level"] != "warn" {
		t.Fatalf("unexpected stream labels: %v", stream.Stream)
	}
	if _, ok := stream.Stream["hostname"]; ok {
		t.Fatalf("non-indexed field leaked into stream labels: %v", stream.Stream)
	}
	if len(stream.Values) != 1 {
		t.Fatalf("expected 1 value tuple, got %d", len(stream.Values))
	}

	var tuple []json.RawMessage
	if err := json.Unmarshal(stream.Values[0], &tuple); err != nil {
		t.Fatalf("failed to decode tuple: %v raw=%s", err, stream.Values[0])
	}
	if len(tuple) != 3 {
		t.Fatalf("expected 3-tuple [ts,line,metadata], got len=%d raw=%s", len(tuple), stream.Values[0])
	}

	var meta map[string]map[string]string
	if err := json.Unmarshal(tuple[2], &meta); err != nil {
		t.Fatalf("failed to decode tuple metadata: %v raw=%s", err, tuple[2])
	}
	if meta["parsed"]["parse_format"] != "klog" {
		t.Fatalf("parsed metadata = %v", meta["parsed"])
	}
	if meta["parsed"]["labels.component"] != "apiserver" ||
		meta["parsed"]["hostname"] != "node-1" {
		t.Fatalf("parsed metadata = %v", meta["parsed"])
	}
	if _, ok := meta["structuredMetadata"]; ok {
		t.Fatalf("structuredMetadata should be empty in categorized shaping: %v", meta["structuredMetadata"])
	}
}

func TestQueryRangeDrilldownDetailsQueryUsesCategorizedResponseWithoutHeader(t *testing.T) {
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			return fn(vlogs.Record{
				"_time":            "2024-01-15T12:00:00Z",
				"_msg":             "Reconciler error",
				"container":        "victoriametrics-operator",
				"namespace":        "monitoring",
				"level":            "error",
				"parse_format":     "json",
				"parse_status":     "success",
				"labels.component": "victoriametrics",
				"hostname":         "logging-fluentbit-q5qzg",
			})
		},
	})
	deps.Cfg.Labels.KnownLabels = []string{"service_name", "detected_level", "namespace", "container", "nodename"}

	ctx := newCtx(`/loki/api/v1/query_range?query={service_name="victoriametrics-operator"} | json | logfmt | drop __error__, __error_details__&start=1705320000&end=1705323600`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	var body struct {
		Status string `json:"status"`
		Data   struct {
			EncodingFlags []string `json:"encodingFlags"`
			Result        []struct {
				Values []json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(ctx.Response.Body(), &body); err != nil {
		t.Fatalf("json.Unmarshal(): %v body=%s", err, ctx.Response.Body())
	}
	if !slices.Contains(body.Data.EncodingFlags, "categorize-labels") {
		t.Fatalf("encodingFlags = %v, want categorize-labels", body.Data.EncodingFlags)
	}
	if len(body.Data.Result) != 1 || len(body.Data.Result[0].Values) != 1 {
		t.Fatalf("unexpected result shape: %+v", body.Data.Result)
	}

	var tuple []json.RawMessage
	if err := json.Unmarshal(body.Data.Result[0].Values[0], &tuple); err != nil {
		t.Fatalf("failed to decode tuple: %v raw=%s", err, body.Data.Result[0].Values[0])
	}
	if len(tuple) != 3 {
		t.Fatalf("expected 3-tuple for drilldown details query, got len=%d raw=%s", len(tuple), body.Data.Result[0].Values[0])
	}
}

func TestQueryRangeDoesNotRestrictReturnedFieldsByKnownLabels(t *testing.T) {
	var got vlogs.LogQueryRequest
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			got = req
			return fn(vlogs.Record{
				"_time":        "2024-01-15T12:00:00Z",
				"_msg":         "log line",
				"service_name": "istiod",
				"container":    "istiod",
				"parse_format": "klog",
				"parse_status": "success",
			})
		},
	})
	deps.Cfg.Labels.KnownLabels = []string{"service_name", "container"}

	ctx := newCtx(`/loki/api/v1/query_range?query={service_name="istiod"}&start=1705320000&end=1705323600`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if got.Query == "" {
		t.Fatal("expected translated VL query to be populated")
	}

	body := decodeBody[loki.StreamsResponse](t, ctx)
	if len(body.Data.Result) != 1 {
		t.Fatalf("stream count = %d, want 1", len(body.Data.Result))
	}
	stream := body.Data.Result[0].Stream
	if stream["service_name"] != "istiod" || stream["container"] != "istiod" {
		t.Fatalf("unexpected core stream labels: %+v", stream)
	}
	if _, ok := stream["parse_format"]; ok {
		t.Fatalf("parse_format should not be part of compact stream labels: %+v", stream)
	}
	if _, ok := stream["parse_status"]; ok {
		t.Fatalf("parse_status should not be part of compact stream labels: %+v", stream)
	}
}

func TestQueryRangeAcceptsGrafanaPatternDetailsPipeline(t *testing.T) {
	var got vlogs.LogQueryRequest
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			got = req
			return fn(vlogs.Record{
				"_time":     "2024-01-15T12:00:00Z",
				"_msg":      "1.23e+09 INFO running periodic notificationchannel resync",
				"_stream":   `{container="grafana-operator",namespace="monitoring"}`,
				"container": "grafana-operator",
				"namespace": "monitoring",
				"level":     "info",
				"field_1":   "1.23e+09 INFO running",
			})
		},
	})

	ctx := newCtx(`/loki/api/v1/query_range?direction=backward&end=1705323600000000000&limit=1000&query=%7Bservice_name%3D%22grafana-operator%22%7D+%7C%3E+%60%3C_%3E+periodic+notificationchannel+resync%60+%7C+pattern+%60%3Cfield_1%3E+periodic+notificationchannel+resync%60+%7C+keep+field_1+%7C+line_format+%22%22&start=1705320000000000000&step=82ms`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(got.Query, `_msg:~`) {
		t.Fatalf("translated query = %q, want pattern regex matcher", got.Query)
	}
	if !strings.Contains(got.Query, `| extract "<field_1> periodic notificationchannel resync"`) {
		t.Fatalf("translated query = %q, want extract pipeline", got.Query)
	}
	if !strings.Contains(got.Query, `| fields _time, _msg, _stream, field_1 | format ""`) {
		t.Fatalf("translated query = %q, want fields/format pipeline", got.Query)
	}

	var body struct {
		Data struct {
			EncodingFlags []string `json:"encodingFlags"`
			Result        []struct {
				Values []json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(ctx.Response.Body(), &body); err != nil {
		t.Fatalf("json.Unmarshal(): %v body=%s", err, ctx.Response.Body())
	}
	if !slices.Contains(body.Data.EncodingFlags, "categorize-labels") {
		t.Fatalf("encodingFlags = %v, want categorize-labels", body.Data.EncodingFlags)
	}
	var tuple []json.RawMessage
	if err := json.Unmarshal(body.Data.Result[0].Values[0], &tuple); err != nil {
		t.Fatalf("failed to decode tuple: %v raw=%s", err, body.Data.Result[0].Values[0])
	}
	if len(tuple) != 3 {
		t.Fatalf("expected 3-tuple for pattern stats query, got len=%d raw=%s", len(tuple), body.Data.Result[0].Values[0])
	}
	var meta map[string]map[string]string
	if err := json.Unmarshal(tuple[2], &meta); err != nil {
		t.Fatalf("failed to decode tuple metadata: %v raw=%s", err, tuple[2])
	}
	if meta["parsed"]["field_1"] != "1.23e+09 INFO running" {
		t.Fatalf("parsed metadata = %v, want field_1", meta["parsed"])
	}
}

func TestQueryRangeInternalStreamSelectorUsesNativeVLogsSyntax(t *testing.T) {
	var got vlogs.LogQueryRequest
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			got = req
			return fn(vlogs.Record{
				"_time":     "2024-01-15T12:00:00Z",
				"_msg":      "log line",
				"container": "cloud-provider-kind",
				"namespace": "kube-system",
			})
		},
	})

	ctx := newCtx("/loki/api/v1/query_range?query=%7B_stream%3D%22%7Bcontainer%3D%5C%22cloud-provider-kind%5C%22%2Cnamespace%3D%5C%22kube-system%5C%22%7D%22%7D+%7C%3D+%60%60&start=1705320000&end=1705323600")
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if got.Query != `{container="cloud-provider-kind",namespace="kube-system"}` {
		t.Fatalf("translated query = %q, want native _stream selector", got.Query)
	}

	body := decodeBody[loki.StreamsResponse](t, ctx)
	if len(body.Data.Result) != 1 {
		t.Fatalf("stream count = %d, want 1", len(body.Data.Result))
	}
}

func TestQueryHealthCheckVectorExprReturnsSingleSample(t *testing.T) {
	deps := testDeps(&stubVL{})

	ctx := newCtx(`/loki/api/v1/query?query=vector(1)%2Bvector(1)&time=4000000000`)
	deps.Query(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.VectorResponse](t, ctx)
	if body.Status != "success" {
		t.Fatalf("status = %q, want success", body.Status)
	}
	if body.Data.ResultType != "vector" {
		t.Fatalf("resultType = %q, want vector", body.Data.ResultType)
	}
	if len(body.Data.Result) != 1 {
		t.Fatalf("result len = %d, want 1", len(body.Data.Result))
	}
	if len(body.Data.Result[0].Value) != 2 {
		t.Fatalf("value len = %d, want 2", len(body.Data.Result[0].Value))
	}
	if got, ok := body.Data.Result[0].Value[1].(string); !ok || got != "2" {
		t.Fatalf("sample value = %#v, want string %q", body.Data.Result[0].Value[1], "2")
	}
}

func TestLabelsUsesParsedTimeRangeAndCachesSuccess(t *testing.T) {
	start := time.Unix(1705320000, 0).UTC()
	end := time.Unix(1705323600, 0).UTC()

	callCount := 0
	var got vlogs.FieldNamesRequest
	deps := testDeps(&stubVL{
		fieldNamesFn: func(_ context.Context, req vlogs.FieldNamesRequest) ([]string, error) {
			callCount++
			got = req
			return []string{"app", "level"}, nil
		},
	})

	first := newCtx(`/loki/api/v1/labels?start=1705320000&end=1705323600`)
	deps.Labels(first)
	if first.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("first status = %d, want 200", first.Response.StatusCode())
	}
	if got.Query != "*" {
		t.Errorf("FieldNames query = %q, want *", got.Query)
	}
	if !got.Start.Equal(start) || !got.End.Equal(end) {
		t.Errorf("FieldNames range = [%v, %v], want [%v, %v]", got.Start, got.End, start, end)
	}

	second := newCtx(`/loki/api/v1/labels?start=1705320000&end=1705323600`)
	deps.Labels(second)
	if second.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("second status = %d, want 200", second.Response.StatusCode())
	}
	if callCount != 1 {
		t.Errorf("FieldNames call count = %d, want 1 (second request served from cache)", callCount)
	}
}

func TestLabelsErrorIsNotCached(t *testing.T) {
	callCount := 0
	deps := testDeps(&stubVL{
		fieldNamesFn: func(_ context.Context, req vlogs.FieldNamesRequest) ([]string, error) {
			callCount++
			return nil, errors.New("boom")
		},
	})

	for i := 0; i < 2; i++ {
		ctx := newCtx(`/loki/api/v1/labels?start=1705320000&end=1705323600`)
		deps.Labels(ctx)
		if ctx.Response.StatusCode() != fasthttp.StatusBadGateway {
			t.Fatalf("request %d status = %d, want 502", i+1, ctx.Response.StatusCode())
		}
	}

	if callCount != 2 {
		t.Errorf("FieldNames call count = %d, want 2 (errors must not be cached)", callCount)
	}
}

func TestDetectedFieldValuesUsesRemapAndScopedQuery(t *testing.T) {
	var got vlogs.FieldValuesRequest
	deps := testDeps(&stubVL{
		fieldValuesFn: func(_ context.Context, req vlogs.FieldValuesRequest) ([]string, error) {
			got = req
			return []string{"info", "warn"}, nil
		},
	})

	ctx := newCtx(`/loki/api/v1/detected_field/detected_level/values?start=1705320000&end=1705323600&limit=1000&query={service_name="vmalert"} | json | logfmt | drop __error__, __error_details__`)
	deps.BuildHandler()(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if got.FieldName != "level" {
		t.Fatalf("FieldName = %q, want %q", got.FieldName, "level")
	}
	if got.Query == "*" {
		t.Fatalf("Query = %q, want scoped filter", got.Query)
	}

	body := decodeBody[loki.LabelValuesResponse](t, ctx)
	if len(body.Data) != 2 || body.Data[0] != "info" || body.Data[1] != "warn" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestDetectedFieldValuesNormalizesDetectedLevelValues(t *testing.T) {
	deps := testDeps(&stubVL{
		fieldValuesFn: func(_ context.Context, req vlogs.FieldValuesRequest) ([]string, error) {
			return []string{"err", "error", "INFO"}, nil
		},
	})

	ctx := newCtx(`/loki/api/v1/detected_field/detected_level/values?start=1705320000&end=1705323600&query={service_name="vmalert"}`)
	ctx.SetUserValue("name", "detected_level")
	deps.DetectedFieldValues(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.LabelValuesResponse](t, ctx)
	if len(body.Data) != 2 || body.Data[0] != "error" || body.Data[1] != "info" {
		t.Fatalf("unexpected normalized values: %+v", body.Data)
	}
}

func TestLabelValuesUsesRemapForSyntheticLabelName(t *testing.T) {
	var got vlogs.FieldValuesRequest
	deps := testDeps(&stubVL{
		fieldValuesFn: func(_ context.Context, req vlogs.FieldValuesRequest) ([]string, error) {
			got = req
			return []string{"info", "warn"}, nil
		},
	})

	ctx := newCtx(`/loki/api/v1/label/detected_level/values?start=1705320000&end=1705323600`)
	deps.BuildHandler()(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if got.FieldName != "level" {
		t.Fatalf("FieldName = %q, want %q", got.FieldName, "level")
	}
}

func TestLabelValuesSynthesizesServiceNameValues(t *testing.T) {
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			records := []vlogs.Record{
				{"_time": "2024-01-15T12:00:00Z", "_msg": "r1", "container": "backend"},
				{"_time": "2024-01-15T12:00:01Z", "_msg": "r2", "labels.app.kubernetes.io/name": "frontend"},
				{"_time": "2024-01-15T12:00:02Z", "_msg": "r3", "container": "backend"},
			}
			for _, rec := range records {
				if err := fn(rec); err != nil {
					return err
				}
			}
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/label/service_name/values?start=1705320000&end=1705323600`)
	deps.BuildHandler()(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.LabelValuesResponse](t, ctx)
	if len(body.Data) != 2 || body.Data[0] != "backend" || body.Data[1] != "frontend" {
		t.Fatalf("unexpected service_name values: %+v", body.Data)
	}
}

func TestAggregationReturnsOriginalLabelNameAfterRemap(t *testing.T) {
	base := time.Unix(1705320000, 0).UTC()
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			if req.Query != `app:="api"` {
				t.Fatalf("Query = %q, want %q", req.Query, `app:="api"`)
			}
			records := []vlogs.Record{
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r1", "level": "info"},
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r2", "level": "error"},
			}
			for _, rec := range records {
				if err := fn(rec); err != nil {
					return err
				}
			}
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/query_range?query=sum+by+(detected_level)+(count_over_time({app="api"}[2s]))&start=1705320000&end=1705323600&step=2`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.MatrixResponse](t, ctx)
	if len(body.Data.Result) != 2 {
		t.Fatalf("expected 2 series, got %d", len(body.Data.Result))
	}
	for _, series := range body.Data.Result {
		if _, ok := series.Metric["detected_level"]; !ok {
			t.Fatalf("metric missing detected_level: %+v", series.Metric)
		}
		if _, ok := series.Metric["level"]; ok {
			t.Fatalf("metric unexpectedly exposes remapped key 'level': %+v", series.Metric)
		}
	}
}

func TestAggregationNormalizesDetectedLevelFromRawLevelField(t *testing.T) {
	base := time.Unix(1705320000, 0).UTC()
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			records := []vlogs.Record{
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r1", "level": "err"},
			}
			for _, rec := range records {
				if err := fn(rec); err != nil {
					return err
				}
			}
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/query_range?query=sum+by+(detected_level)+(count_over_time({app="api"}[2s]))&start=1705320000&end=1705323600&step=2`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.MatrixResponse](t, ctx)
	if len(body.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(body.Data.Result))
	}
	if got := body.Data.Result[0].Metric["detected_level"]; got != "error" {
		t.Fatalf("detected_level = %q, want %q", got, "error")
	}
}

func TestAggregationSynthesizesServiceNameFromFallbackFields(t *testing.T) {
	base := time.Unix(1705320000, 0).UTC()
	var gotQuery string
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			gotQuery = req.Query
			records := []vlogs.Record{
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r1", "container": "qubership-logql-to-logsql-proxy"},
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r2", "container": "qubership-logql-to-logsql-proxy"},
			}
			for _, rec := range records {
				if err := fn(rec); err != nil {
					return err
				}
			}
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/query_range?query=sum+%28count_over_time%28%7Bcontainer%3D%22qubership-logql-to-logsql-proxy%22+%2Cservice_name+%21%3D+%22%22%7D+%5B2s%5D%29%29+by+%28service_name%29&start=1705320000&end=1705323600&step=2`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(gotQuery, `container:="qubership-logql-to-logsql-proxy"`) {
		t.Fatalf("translated query = %q, want container selector retained", gotQuery)
	}
	if !strings.Contains(gotQuery, `container:~".+"`) {
		t.Fatalf("translated query = %q, want synthetic service_name non-empty matcher", gotQuery)
	}

	body := decodeBody[loki.MatrixResponse](t, ctx)
	if len(body.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(body.Data.Result))
	}
	if got := body.Data.Result[0].Metric["service_name"]; got != "qubership-logql-to-logsql-proxy" {
		t.Fatalf("service_name = %q, want %q", got, "qubership-logql-to-logsql-proxy")
	}
	if got := body.Data.Result[0].Values[0][1]; got != "2" {
		t.Fatalf("bucket count = %v, want %q", got, "2")
	}
}

func TestAggregationCountByCountsDistinctSourceStreams(t *testing.T) {
	base := time.Unix(1705320000, 0).UTC()
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			records := []vlogs.Record{
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r1", "_stream": `{pod="a"}`, "level": "info"},
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r2", "_stream": `{pod="a"}`, "level": "info"},
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r3", "_stream": `{pod="b"}`, "level": "info"},
			}
			for _, rec := range records {
				if err := fn(rec); err != nil {
					return err
				}
			}
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/query_range?query=count+by+(detected_level)+(count_over_time({app="api"}[2s]))&start=1705320000&end=1705323600&step=2`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.MatrixResponse](t, ctx)
	if len(body.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(body.Data.Result))
	}
	if got := body.Data.Result[0].Values[0][1]; got != "2" {
		t.Fatalf("count by bucket = %v, want %q", got, "2")
	}
}

func TestAggregationAvgByAveragesPerSourceStream(t *testing.T) {
	base := time.Unix(1705320000, 0).UTC()
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			records := []vlogs.Record{
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r1", "_stream": `{pod="a"}`, "level": "info"},
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r2", "_stream": `{pod="a"}`, "level": "info"},
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r3", "_stream": `{pod="b"}`, "level": "info"},
			}
			for _, rec := range records {
				if err := fn(rec); err != nil {
					return err
				}
			}
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/query_range?query=avg+by+(detected_level)+(count_over_time({app="api"}[2s]))&start=1705320000&end=1705323600&step=2`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.MatrixResponse](t, ctx)
	if len(body.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(body.Data.Result))
	}
	if got := body.Data.Result[0].Values[0][1]; got != "1.5" {
		t.Fatalf("avg by bucket = %v, want %q", got, "1.5")
	}
}

func TestAggregationRateBySumsPerSecondRates(t *testing.T) {
	base := time.Unix(1705320000, 0).UTC()
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			records := []vlogs.Record{
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r1", "_stream": `{pod="a"}`, "level": "info"},
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r2", "_stream": `{pod="a"}`, "level": "info"},
				{"_time": base.Format(time.RFC3339Nano), "_msg": "r3", "_stream": `{pod="b"}`, "level": "info"},
			}
			for _, rec := range records {
				if err := fn(rec); err != nil {
					return err
				}
			}
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/query_range?query=sum+by+(detected_level)+(rate({app="api"}[2s]))&start=1705320000&end=1705323600&step=2`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.MatrixResponse](t, ctx)
	if len(body.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(body.Data.Result))
	}
	if got := body.Data.Result[0].Values[0][1]; got != "1.5" {
		t.Fatalf("sum rate bucket = %v, want %q", got, "1.5")
	}
}

func TestAggregationDoesNotApplyArtificialQueryLimit(t *testing.T) {
	var got vlogs.LogQueryRequest
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			got = req
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/query_range?query=sum+by+(detected_level)+(count_over_time({app="api"}[2s]))&start=1705320000&end=1705323600&step=2`)
	deps.QueryRange(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if got.Limit != 0 {
		t.Fatalf("Limit = %d, want 0 (unbounded backend scan)", got.Limit)
	}
}

func TestPatternsBuildsCollapsedQueryAndReturnsTopPattern(t *testing.T) {
	var gotQuery string
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			gotQuery = req.Query
			records := []vlogs.Record{
				{"_time": "2024-01-15T12:00:00Z", "_msg": "error <N> for user", "app": "api"},
				{"_time": "2024-01-15T12:00:30Z", "_msg": "error <N> for user", "app": "api"},
				{"_time": "2024-01-15T12:01:00Z", "_msg": "ok", "app": "api"},
			}
			for _, rec := range records {
				if err := fn(rec); err != nil {
					return err
				}
			}
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/patterns?query={app="api",level!="debug"}&start=1705320000&end=1705323600&step=60&limit=1`)
	deps.Patterns(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(gotQuery, `collapse_nums prettify | fields _msg, _time`) {
		t.Errorf("query = %q, want collapse/prettify suffix", gotQuery)
	}

	body := decodeBody[loki.PatternsResponse](t, ctx)
	if len(body.Data) != 1 {
		t.Fatalf("patterns len = %d, want 1", len(body.Data))
	}
	if body.Data[0].Pattern != "error <N> for user" {
		t.Errorf("pattern = %q, want top pattern", body.Data[0].Pattern)
	}
	if body.Data[0].Labels["app"] != "api" {
		t.Errorf("labels = %v, want app=api", body.Data[0].Labels)
	}
	if _, ok := body.Data[0].Labels["level"]; ok {
		t.Errorf("labels = %v, did not expect non-equality matcher", body.Data[0].Labels)
	}
}

func TestPatternsNormalizesScientificNotationAndExtractsDetectedLevel(t *testing.T) {
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			records := []vlogs.Record{
				{"_time": "2024-01-15T12:00:00Z", "_msg": "<N>.<N>+<N> INFO running periodic dashboard resync"},
				{"_time": "2024-01-15T12:00:30Z", "_msg": "<N>.7787491734332945e+<N> INFO running periodic dashboard resync"},
				{"_time": "2024-01-15T12:01:00Z", "_msg": "<N>.<N>+<N> INFO running periodic dashboardfolder resync"},
			}
			for _, rec := range records {
				if err := fn(rec); err != nil {
					return err
				}
			}
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/patterns?query={service_name="grafana-operator"}&start=1705320000&end=1705323600&step=60&limit=10`)
	deps.Patterns(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.PatternsResponse](t, ctx)
	if len(body.Data) != 2 {
		t.Fatalf("patterns len = %d, want 2", len(body.Data))
	}
	if body.Data[0].Pattern != "<_> periodic dashboard resync" {
		t.Fatalf("first pattern = %q, want normalized periodic pattern", body.Data[0].Pattern)
	}
	if body.Data[0].Labels["detected_level"] != "info" {
		t.Fatalf("detected_level = %q, want info", body.Data[0].Labels["detected_level"])
	}
	gotCount, ok := body.Data[0].Samples[0][1].(float64)
	if !ok {
		t.Fatalf("first sample count type = %T, want float64", body.Data[0].Samples[0][1])
	}
	if gotCount != 2 {
		t.Fatalf("first sample count = %v, want 2", gotCount)
	}
}

func TestDetectedFieldsUsesBestEffortFilter(t *testing.T) {
	var gotFilter string
	deps := testDeps(&stubVL{
		fieldNamesFn: func(_ context.Context, req vlogs.FieldNamesRequest) ([]string, error) {
			gotFilter = req.Query
			return []string{"app", "level"}, nil
		},
	})

	ctx := newCtx(`/loki/api/v1/detected_fields?query={app="api"}|line_format "{{.msg}}"&start=1705320000&end=1705323600`)
	deps.DetectedFields(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if gotFilter != `app:="api"` {
		t.Errorf("filter = %q, want %q", gotFilter, `app:="api"`)
	}

	body := decodeBody[loki.DetectedFieldsResponse](t, ctx)
	if len(body.Fields) != 2 || body.Fields[0].Type != "string" {
		t.Errorf("unexpected fields response: %+v", body.Fields)
	}
}

func TestDetectedFieldsExcludesKnownLabelsAndInternalFields(t *testing.T) {
	deps := testDeps(&stubVL{
		fieldNamesFn: func(_ context.Context, req vlogs.FieldNamesRequest) ([]string, error) {
			return []string{"service_name", "container", "level", "_msg", "_time", "parse_format", "parse_status"}, nil
		},
	})
	deps.Cfg.Labels.KnownLabels = []string{"service_name", "container", "detected_level"}

	ctx := newCtx(`/loki/api/v1/detected_fields?query={service_name="vmalert"}&start=1705320000&end=1705323600`)
	deps.DetectedFields(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.DetectedFieldsResponse](t, ctx)
	got := make([]string, 0, len(body.Fields))
	for _, f := range body.Fields {
		got = append(got, f.Label)
	}
	want := []string{"parse_format", "parse_status"}
	if !slices.Equal(got, want) {
		t.Fatalf("fields = %v, want %v", got, want)
	}
}

func TestDetectedFieldsSuppressesNoisyTerminalAndObjectLikeFields(t *testing.T) {
	deps := testDeps(&stubVL{
		fieldNamesFn: func(_ context.Context, req vlogs.FieldNamesRequest) ([]string, error) {
			return []string{
				"timestamp_end",
				"observed_timestamp_end",
				"service={name:\"api\"}",
				"certificate:",
				"good_field",
				"labels.component",
			}, nil
		},
	})

	ctx := newCtx(`/loki/api/v1/detected_fields?query={service_name="vmalert"}&start=1705320000&end=1705323600`)
	deps.DetectedFields(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.DetectedFieldsResponse](t, ctx)
	got := make([]string, 0, len(body.Fields))
	for _, f := range body.Fields {
		got = append(got, f.Label)
	}
	want := []string{"labels.component", "good_field"}
	if !slices.Equal(got, want) {
		t.Fatalf("fields = %v, want %v", got, want)
	}
}

func TestDetectedFieldsPrioritizesParsedThenStructuredThenUnclassified(t *testing.T) {
	deps := testDeps(&stubVL{
		fieldNamesFn: func(_ context.Context, req vlogs.FieldNamesRequest) ([]string, error) {
			return []string{"z_field", "labels.component", "parse_status", "parse_format", "a_field"}, nil
		},
	})

	ctx := newCtx(`/loki/api/v1/detected_fields?query={service_name="vmalert"}&start=1705320000&end=1705323600`)
	deps.DetectedFields(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	body := decodeBody[loki.DetectedFieldsResponse](t, ctx)
	got := make([]string, 0, len(body.Fields))
	for _, f := range body.Fields {
		got = append(got, f.Label)
	}
	want := []string{"parse_format", "parse_status", "labels.component", "a_field", "z_field"}
	if !slices.Equal(got, want) {
		t.Fatalf("fields = %v, want %v", got, want)
	}
}

func TestFieldClassificationHelpers(t *testing.T) {
	cfg := config.LabelsConfig{
		KnownLabels:             []string{"service_name", "detected_level"},
		KnownParsedFields:       []string{"parse_format"},
		KnownStructuredMetadata: []string{"labels.component"},
		ExcludedFields:          []string{"_msg"},
		LabelRemap:              map[string]string{"detected_level": "level"},
	}

	tests := map[string]fieldclass.FieldClass{
		"service_name":     fieldclass.FieldClassLabel,
		"level":            fieldclass.FieldClassLabel,
		"parse_format":     fieldclass.FieldClassParsed,
		"labels.component": fieldclass.FieldClassStructuredMetadata,
		"_msg":             fieldclass.FieldClassExcluded,
		"parse_status":     fieldclass.FieldClassUnclassified,
	}
	for field, want := range tests {
		if got := fieldclass.Classify(field, cfg); got != want {
			t.Fatalf("Classify(%q) = %q, want %q", field, got, want)
		}
	}
}

func TestDetectedFieldValuesSuppressesLabelAndExcludedClasses(t *testing.T) {
	deps := testDeps(&stubVL{
		fieldValuesFn: func(_ context.Context, req vlogs.FieldValuesRequest) ([]string, error) {
			t.Fatalf("FieldValues should not be called for suppressed field classes: %+v", req)
			return nil, nil
		},
	})
	deps.Cfg.Labels.KnownLabels = []string{"service_name"}
	deps.Cfg.Labels.ExcludedFields = []string{"_msg"}

	for _, path := range []string{
		`/loki/api/v1/detected_field/service_name/values?start=1705320000&end=1705323600`,
		`/loki/api/v1/detected_field/_msg/values?start=1705320000&end=1705323600`,
	} {
		ctx := newCtx(path)
		deps.BuildHandler()(ctx)
		if ctx.Response.StatusCode() != fasthttp.StatusOK {
			t.Fatalf("%s status = %d, want 200 body=%s", path, ctx.Response.StatusCode(), ctx.Response.Body())
		}
		body := decodeBody[loki.LabelValuesResponse](t, ctx)
		if len(body.Data) != 0 {
			t.Fatalf("%s body = %+v, want empty success payload", path, body)
		}
	}
}

func TestDetectedFieldsIgnoresParserStagesForFieldDiscovery(t *testing.T) {
	var gotFilter string
	deps := testDeps(&stubVL{
		fieldNamesFn: func(_ context.Context, req vlogs.FieldNamesRequest) ([]string, error) {
			gotFilter = req.Query
			return []string{"container", "level"}, nil
		},
	})

	ctx := newCtx(`/loki/api/v1/detected_fields?query={service_name="vmalert"} | json | logfmt | drop __error__, __error_details__&start=1705320000&end=1705323600`)
	deps.DetectedFields(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if strings.Contains(gotFilter, "unpack_logfmt") {
		t.Fatalf("filter = %q, must not include unpack_logfmt", gotFilter)
	}
	if gotFilter == "*" {
		t.Fatalf("filter = %q, want selector-scoped filter", gotFilter)
	}
}

func TestDetectedFieldsErrorReturnsBadGateway(t *testing.T) {
	deps := testDeps(&stubVL{
		fieldNamesFn: func(_ context.Context, req vlogs.FieldNamesRequest) ([]string, error) {
			return nil, errors.New("metadata backend down")
		},
	})

	ctx := newCtx(`/loki/api/v1/detected_fields?query={app="api"}&start=1705320000&end=1705323600`)
	deps.DetectedFields(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusBadGateway {
		t.Fatalf("status = %d, want 502", ctx.Response.StatusCode())
	}
	body := decodeBody[loki.ErrorResponse](t, ctx)
	if body.Status != "error" || body.ErrorType != "execution" {
		t.Errorf("unexpected error body: %+v", body)
	}
}

func TestHelpersCoverFallbackAndParsing(t *testing.T) {
	if got := bestEffortLogsQLFilter("", translator.Options{}); got != "*" {
		t.Errorf("empty filter = %q, want *", got)
	}
	if got := bestEffortLogsQLFilter(`{app="api"} | line_format "{{.msg}}"`, translator.Options{}); got != `app:="api"` {
		t.Errorf("fallback filter = %q, want %q", got, `app:="api"`)
	}
	if got := bestEffortLogsQLFilter(`not logql`, translator.Options{}); got != "*" {
		t.Errorf("invalid filter = %q, want *", got)
	}
	if got := selectorOnlyLogsQLFilter(`{service_name="vmalert"} | json | logfmt`, translator.Options{}); got == "*" {
		t.Errorf("selectorOnlyLogsQLFilter() = %q, want scoped selector filter", got)
	}
	if got := extractStreamSelector(`sum(rate({app="api"}[5m]))`); got != `{app="api"}` {
		t.Errorf("extractStreamSelector() = %q", got)
	}
	if got := extractStreamSelector(`no selector`); got != "" {
		t.Errorf("extractStreamSelector(no selector) = %q, want empty", got)
	}

	for _, tc := range []struct {
		in   string
		want time.Time
	}{
		{"1705320000", time.Unix(1705320000, 0).UTC()},
		{"1705320000123", time.Unix(1705320000, 123000000).UTC()},
		{"1705320000.5", time.Unix(1705320000, 500000000).UTC()},
		{"2024-01-15T12:00:00Z", time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)},
	} {
		got, err := parseTime(tc.in)
		if err != nil {
			t.Fatalf("parseTime(%q): %v", tc.in, err)
		}
		if !got.Equal(tc.want) {
			t.Errorf("parseTime(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if _, err := parseTime("bad"); err == nil {
		t.Fatal("expected parseTime(bad) error")
	}

	if got := parseDuration("120"); got != 120*time.Second {
		t.Errorf("parseDuration(120) = %v, want 120s", got)
	}
	if got := parseDuration("2m"); got != 2*time.Minute {
		t.Errorf("parseDuration(2m) = %v, want 2m", got)
	}
	if got := parseDuration("bad"); got != time.Minute {
		t.Errorf("parseDuration(bad) = %v, want 1m", got)
	}
}

func TestRecoveryAndLoggingMiddleware(t *testing.T) {
	panicCtx := newCtx("/boom")
	RecoveryMiddleware(func(*fasthttp.RequestCtx) {
		panic("boom")
	})(panicCtx)
	if panicCtx.Response.StatusCode() != fasthttp.StatusInternalServerError {
		t.Fatalf("recovery status = %d, want 500", panicCtx.Response.StatusCode())
	}

	logCtx := newCtx("/loki/api/v1/query?query={app=\"api\"}")
	LoggingMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusAccepted)
	})(logCtx)
	if logCtx.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("logging middleware status = %d, want 202", logCtx.Response.StatusCode())
	}

	readyCtx := newCtx("/ready")
	LoggingMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	})(readyCtx)
	if readyCtx.Response.StatusCode() != fasthttp.StatusNoContent {
		t.Fatalf("ready status = %d, want 204", readyCtx.Response.StatusCode())
	}
}

func TestSelectorEqualityLabels(t *testing.T) {
	q, err := parser.Parse(`sum by (app) (count_over_time({app="api",env=~"prod.*",level!="debug"}[5m]))`)
	if err != nil {
		t.Fatalf("Parse(): %v", err)
	}
	got := selectorEqualityLabels(q)
	if len(got) != 1 || got["app"] != "api" {
		t.Errorf("selectorEqualityLabels() = %v, want only app=api", got)
	}
}

func TestLabelValuesPassesFieldRangeAndLimit(t *testing.T) {
	start := time.Unix(1705320000, 0).UTC()
	end := time.Unix(1705323600, 0).UTC()

	var got vlogs.FieldValuesRequest
	deps := testDeps(&stubVL{
		fieldValuesFn: func(_ context.Context, req vlogs.FieldValuesRequest) ([]string, error) {
			got = req
			return []string{"api"}, nil
		},
	})

	ctx := newCtx(`/loki/api/v1/label/app/values?start=1705320000&end=1705323600`)
	ctx.SetUserValue("name", "app")
	deps.LabelValues(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200", ctx.Response.StatusCode())
	}
	if got.FieldName != "app" {
		t.Errorf("FieldName = %q, want app", got.FieldName)
	}
	if got.Query != "*" {
		t.Errorf("Query = %q, want *", got.Query)
	}
	if got.Limit != deps.Cfg.Limits.MaxLimit {
		t.Errorf("Limit = %d, want %d", got.Limit, deps.Cfg.Limits.MaxLimit)
	}
	if !got.Start.Equal(start) || !got.End.Equal(end) {
		t.Errorf("range = [%v, %v], want [%v, %v]", got.Start, got.End, start, end)
	}
}

func TestSyntheticServiceNameValuesDoesNotApplyArtificialQueryLimit(t *testing.T) {
	var got vlogs.LogQueryRequest
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			got = req
			return nil
		},
	})

	ctx := newCtx(`/loki/api/v1/label/service_name/values?start=1705320000&end=1705323600`)
	ctx.SetUserValue("name", "service_name")
	deps.LabelValues(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200", ctx.Response.StatusCode())
	}
	if got.Limit != 0 {
		t.Fatalf("Limit = %d, want 0 (unbounded backend scan)", got.Limit)
	}
}

func TestLabelValuesErrorIsNotCached(t *testing.T) {
	callCount := 0
	deps := testDeps(&stubVL{
		fieldValuesFn: func(_ context.Context, req vlogs.FieldValuesRequest) ([]string, error) {
			callCount++
			return nil, errors.New("boom")
		},
	})

	for i := 0; i < 2; i++ {
		ctx := newCtx(`/loki/api/v1/label/app/values?start=1705320000&end=1705323600`)
		ctx.SetUserValue("name", "app")
		deps.LabelValues(ctx)
		if ctx.Response.StatusCode() != fasthttp.StatusBadGateway {
			t.Fatalf("request %d status = %d, want 502", i+1, ctx.Response.StatusCode())
		}
	}

	if callCount != 2 {
		t.Errorf("FieldValues call count = %d, want 2 (errors must not be cached)", callCount)
	}
}

func TestLabelValuesAndSeriesHelpers(t *testing.T) {
	deps := testDeps(&stubVL{})

	missingCtx := newCtx(`/loki/api/v1/label//values`)
	deps.LabelValues(missingCtx)
	if missingCtx.Response.StatusCode() != fasthttp.StatusBadRequest {
		t.Fatalf("missing name status = %d, want 400", missingCtx.Response.StatusCode())
	}

	start := time.Unix(1705320000, 0).UTC()
	end := time.Unix(1705323600, 0).UTC()
	key := vlogs.FieldValuesKey("app", start, end)
	deps.Cache.Set(key, []string{"api", "worker"}, time.Minute)

	cacheCtx := newCtx(`/loki/api/v1/label/app/values?start=1705320000&end=1705323600`)
	cacheCtx.SetUserValue("name", "app")
	deps.LabelValues(cacheCtx)
	if cacheCtx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("cached label values status = %d, want 200", cacheCtx.Response.StatusCode())
	}
	labelBody := decodeBody[loki.LabelValuesResponse](t, cacheCtx)
	if len(labelBody.Data) != 2 || labelBody.Data[0] != "api" {
		t.Errorf("cached label values = %v", labelBody.Data)
	}

	var gotQuery string
	seriesDeps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			gotQuery = req.Query
			_ = fn(vlogs.Record{"_time": "2024-01-15T12:00:00Z", "_msg": "a", "app": "api"})
			return vlogs.ErrResponseTooLarge
		},
	})
	seriesCtx := newCtx(`/loki/api/v1/series?match[]={app="api"}&start=1705320000&end=1705323600`)
	seriesDeps.Series(seriesCtx)
	if seriesCtx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("series status = %d, want 200", seriesCtx.Response.StatusCode())
	}
	if gotQuery != `app:="api"` {
		t.Errorf("series query = %q, want %q", gotQuery, `app:="api"`)
	}
	seriesBody := decodeBody[loki.SeriesResponse](t, seriesCtx)
	if len(seriesBody.Data) != 1 || seriesBody.Data[0]["app"] != "api" {
		t.Errorf("series data = %+v", seriesBody.Data)
	}

	if got := deps.seriesFilter(newCtx(`/loki/api/v1/series?match[]=not-logql`)); got != "*" {
		t.Errorf("seriesFilter(invalid) = %q, want *", got)
	}
	if !isLargeOrCancelled(vlogs.ErrResponseTooLarge) || !isLargeOrCancelled(context.Canceled) || isLargeOrCancelled(nil) {
		t.Errorf("unexpected isLargeOrCancelled results")
	}
}

func TestSeriesUsesTranslatedMatchAndConfiguredLimit(t *testing.T) {
	var got vlogs.LogQueryRequest
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			got = req
			return fn(vlogs.Record{
				"_time":          "2024-01-15T12:00:00Z",
				"_msg":           "m",
				"level":          "warn",
				"detected_level": "warn",
			})
		},
	})

	ctx := newCtx(`/loki/api/v1/series?match[]=sum by (detected_level) (count_over_time({app="api",detected_level="warn"}[5m]))&start=1705320000&end=1705323600`)
	deps.Series(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200", ctx.Response.StatusCode())
	}
	if got.Query != `app:="api" AND level:="warn"` {
		t.Errorf("QueryLogs query = %q, want %q", got.Query, `app:="api" AND level:="warn"`)
	}
	if got.Limit != deps.Cfg.Limits.MaxStreamsPerResponse {
		t.Errorf("QueryLogs limit = %d, want %d", got.Limit, deps.Cfg.Limits.MaxStreamsPerResponse)
	}
	body := decodeBody[loki.SeriesResponse](t, ctx)
	if len(body.Data) != 1 || body.Data[0]["level"] != "warn" {
		t.Errorf("unexpected series body: %+v", body.Data)
	}
}

func TestHandleMetricQueryAndParseRecordTime(t *testing.T) {
	var gotStep time.Duration
	deps := testDeps(&stubVL{
		queryHitsFn: func(_ context.Context, req vlogs.HitsQueryRequest) ([]vlogs.HitBucket, error) {
			gotStep = req.Step
			return []vlogs.HitBucket{{Timestamp: time.Unix(1705320000, 0).UTC(), Count: 5}}, nil
		},
	})

	ast, err := parser.Parse(`rate({app="api"}[5m])`)
	if err != nil {
		t.Fatalf("Parse(): %v", err)
	}
	result, err := translator.Translate(ast, translator.Options{})
	if err != nil {
		t.Fatalf("Translate(): %v", err)
	}

	ctx := newCtx(`/loki/api/v1/query_range`)
	deps.handleMetricQuery(ctx, result, ast, time.Unix(1705320000, 0).UTC(), time.Unix(1705323600, 0).UTC(), 0)
	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("handleMetricQuery status = %d, want 200", ctx.Response.StatusCode())
	}
	if gotStep != time.Minute {
		t.Errorf("default step = %v, want 1m", gotStep)
	}

	body := decodeBody[loki.MatrixResponse](t, ctx)
	if body.Data.ResultType != "matrix" || len(body.Data.Result) != 1 {
		t.Errorf("unexpected matrix response: %+v", body.Data)
	}

	if got := parseRecordTime("2024-01-15T12:00:00.123456789Z"); got.Nanosecond() != 123456789 {
		t.Errorf("parseRecordTime(RFC3339Nano) = %v", got)
	}
	if got := parseRecordTime("2024-01-15T12:00:00Z"); got.IsZero() {
		t.Errorf("parseRecordTime(RFC3339) returned zero")
	}
	if got := parseRecordTime("bad"); !got.IsZero() {
		t.Errorf("parseRecordTime(bad) = %v, want zero", got)
	}
}

func TestPatternsCapsLimitAndTimesOut(t *testing.T) {
	var got vlogs.LogQueryRequest
	deps := testDeps(&stubVL{
		queryLogsFn: func(_ context.Context, req vlogs.LogQueryRequest, fn func(vlogs.Record) error) error {
			got = req
			return context.DeadlineExceeded
		},
	})

	ctx := newCtx(`/loki/api/v1/patterns?query={app="api"}&start=1705320000&end=1705323600&step=30&limit=999999`)
	deps.Patterns(ctx)

	if got.Query != `app:="api" | collapse_nums prettify | fields _msg, _time` {
		t.Errorf("QueryLogs query = %q", got.Query)
	}
	if got.Limit != deps.Cfg.Limits.MaxLimit {
		t.Errorf("QueryLogs limit = %d, want capped %d", got.Limit, deps.Cfg.Limits.MaxLimit)
	}
	if ctx.Response.StatusCode() != fasthttp.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", ctx.Response.StatusCode())
	}
	body := decodeBody[loki.ErrorResponse](t, ctx)
	if body.ErrorType != "timeout" {
		t.Errorf("unexpected error body: %+v", body)
	}
}

func TestConcurrencyMiddleware(t *testing.T) {
	lim := limits.New(1, 0)
	if err := lim.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire(): %v", err)
	}

	queueFullCtx := newCtx(`/busy`)
	ConcurrencyMiddleware(lim, time.Second)(func(*fasthttp.RequestCtx) {
		t.Fatal("next handler should not run when queue is full")
	})(queueFullCtx)
	if queueFullCtx.Response.StatusCode() != fasthttp.StatusTooManyRequests {
		t.Fatalf("queue full status = %d, want 429", queueFullCtx.Response.StatusCode())
	}
	lim.Release()

	successLim := limits.New(1, 1)
	successCtx := newCtx(`/ok`)
	ran := false
	ConcurrencyMiddleware(successLim, 2*time.Second)(func(ctx *fasthttp.RequestCtx) {
		ran = true
		if _, ok := reqContext(ctx).Deadline(); !ok {
			t.Error("expected middleware context with deadline")
		}
		ctx.SetStatusCode(fasthttp.StatusCreated)
	})(successCtx)
	if !ran {
		t.Fatal("expected wrapped handler to run")
	}
	if successCtx.Response.StatusCode() != fasthttp.StatusCreated {
		t.Fatalf("success status = %d, want 201", successCtx.Response.StatusCode())
	}
}
