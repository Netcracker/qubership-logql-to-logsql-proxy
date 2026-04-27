package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/config"
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
