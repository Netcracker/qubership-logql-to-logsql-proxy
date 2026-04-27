package vlogs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/config"
)

// ────────────────────────────────────────────────────────────────────────────
// StreamDecoder tests
// ────────────────────────────────────────────────────────────────────────────

func TestStreamDecoderSingleRecord(t *testing.T) {
	ndjson := `{"_msg":"hello","_time":"2024-01-15T12:00:00Z","app":"api"}` + "\n"
	var records []Record
	err := StreamDecoder(context.Background(), strings.NewReader(ndjson), 1<<20, func(r Record) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0]["_msg"] != "hello" {
		t.Errorf("_msg = %q, want %q", records[0]["_msg"], "hello")
	}
	if records[0]["app"] != "api" {
		t.Errorf("app = %q, want %q", records[0]["app"], "api")
	}
}

func TestStreamDecoderMultipleRecords(t *testing.T) {
	lines := []string{
		`{"_msg":"line1","_time":"2024-01-15T12:00:00Z"}`,
		`{"_msg":"line2","_time":"2024-01-15T12:00:01Z"}`,
		`{"_msg":"line3","_time":"2024-01-15T12:00:02Z"}`,
	}
	ndjson := strings.Join(lines, "\n") + "\n"

	var count int
	err := StreamDecoder(context.Background(), strings.NewReader(ndjson), 1<<20, func(r Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 records, got %d", count)
	}
}

func TestStreamDecoderContextCancellation(t *testing.T) {
	// Build a large stream; cancel after reading the first record.
	var lines []string
	for i := 0; i < 100; i++ {
		lines = append(lines, fmt.Sprintf(`{"_msg":"line%d","_time":"2024-01-15T12:00:00Z"}`, i))
	}
	ndjson := strings.Join(lines, "\n") + "\n"

	ctx, cancel := context.WithCancel(context.Background())

	var count int
	err := StreamDecoder(ctx, strings.NewReader(ndjson), 1<<20, func(r Record) error {
		count++
		if count == 1 {
			cancel() // cancel after the first record
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestStreamDecoderMaxBytesEnforced(t *testing.T) {
	// Build a stream whose total size exceeds our maxBytes limit.
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, `{"_msg":"this is a fairly long log line to pad the byte count","_time":"2024-01-15T12:00:00Z"}`)
	}
	ndjson := strings.Join(lines, "\n") + "\n"

	// Set maxBytes to just 50 bytes — much less than the full payload.
	err := StreamDecoder(context.Background(), strings.NewReader(ndjson), 50, func(_ Record) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected ErrResponseTooLarge, got nil")
	}
	if err != ErrResponseTooLarge {
		t.Errorf("expected ErrResponseTooLarge, got %v", err)
	}
}

func TestStreamDecoderMalformedJSON(t *testing.T) {
	ndjson := "not valid json\n"
	err := StreamDecoder(context.Background(), strings.NewReader(ndjson), 1<<20, func(_ Record) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestStreamDecoderSkipsBlankLines(t *testing.T) {
	ndjson := "\n" + `{"_msg":"hi","_time":"2024-01-15T12:00:00Z"}` + "\n\n"
	var count int
	err := StreamDecoder(context.Background(), strings.NewReader(ndjson), 1<<20, func(_ Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 record, got %d", count)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers for client tests
// ────────────────────────────────────────────────────────────────────────────

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// newTestClient creates a Client with an in-memory HTTP transport.
func newTestClient(rt http.RoundTripper, extra ...func(*config.VLogsConfig)) *Client {
	cfg := config.VLogsConfig{
		URL:             "http://victorialogs.test",
		MaxIdleConns:    10,
		MaxConnsPerHost: 5,
	}
	for _, fn := range extra {
		fn(&cfg)
	}
	cl := NewClient(cfg, 64*1024*1024)
	cl.httpCl.Transport = rt
	return cl
}

// captureHandler is an http.Handler that records the last received request.
type captureHandler struct {
	last *http.Request
	body func(http.ResponseWriter, *http.Request)
}

func (h *captureHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.last = r
	if h.body != nil {
		h.body(w, r)
	} else {
		w.Header().Set("Content-Type", "application/json")
		// Empty values array — the object-per-entry format VictoriaLogs uses.
		_, _ = fmt.Fprint(w, `{"values":[]}`)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// decorateRequest tests (exercised indirectly via FieldNames)
// ────────────────────────────────────────────────────────────────────────────

func TestDecorateRequestNoAuth(t *testing.T) {
	h := &captureHandler{}
	cl := newTestClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		h.last = req
		return jsonResponse(http.StatusOK, `{"values":[]}`), nil
	}))
	_, _ = cl.FieldNames(context.Background(), FieldNamesRequest{
		Query: "*",
		Start: time.Now().Add(-time.Hour),
		End:   time.Now(),
	})

	if h.last == nil {
		t.Fatal("no request received")
	}
	if got := h.last.Header.Get("Authorization"); got != "" {
		t.Errorf("expected no Authorization header, got %q", got)
	}
}

func TestDecorateRequestBasicAuth(t *testing.T) {
	h := &captureHandler{}
	cl := newTestClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		h.last = req
		return jsonResponse(http.StatusOK, `{"values":[]}`), nil
	}), func(c *config.VLogsConfig) {
		c.BasicAuth = &config.BasicAuthConfig{Username: "user", Password: "secret"}
	})
	_, _ = cl.FieldNames(context.Background(), FieldNamesRequest{Query: "*", Start: time.Now().Add(-time.Hour), End: time.Now()})

	if h.last == nil {
		t.Fatal("no request received")
	}
	u, p, ok := h.last.BasicAuth()
	if !ok {
		t.Fatal("expected Basic auth, got none")
	}
	if u != "user" || p != "secret" {
		t.Errorf("Basic auth: got %q:%q, want %q:%q", u, p, "user", "secret")
	}
}

func TestDecorateRequestBearerToken(t *testing.T) {
	h := &captureHandler{}
	cl := newTestClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		h.last = req
		return jsonResponse(http.StatusOK, `{"values":[]}`), nil
	}), func(c *config.VLogsConfig) {
		c.BearerToken = "my-token"
	})
	_, _ = cl.FieldNames(context.Background(), FieldNamesRequest{Query: "*", Start: time.Now().Add(-time.Hour), End: time.Now()})

	if h.last == nil {
		t.Fatal("no request received")
	}
	auth := h.last.Header.Get("Authorization")
	if auth != "Bearer my-token" {
		t.Errorf("Authorization = %q, want %q", auth, "Bearer my-token")
	}
}

func TestDecorateRequestExtraHeaders(t *testing.T) {
	h := &captureHandler{}
	cl := newTestClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		h.last = req
		return jsonResponse(http.StatusOK, `{"values":[]}`), nil
	}), func(c *config.VLogsConfig) {
		c.ExtraHeaders = map[string]string{"X-Tenant-ID": "prod", "X-Custom": "flag"}
	})
	_, _ = cl.FieldNames(context.Background(), FieldNamesRequest{Query: "*", Start: time.Now().Add(-time.Hour), End: time.Now()})

	if h.last == nil {
		t.Fatal("no request received")
	}
	if got := h.last.Header.Get("X-Tenant-ID"); got != "prod" {
		t.Errorf("X-Tenant-ID = %q, want %q", got, "prod")
	}
	if got := h.last.Header.Get("X-Custom"); got != "flag" {
		t.Errorf("X-Custom = %q, want %q", got, "flag")
	}
}

func TestDecorateRequestExtraParams(t *testing.T) {
	h := &captureHandler{}
	cl := newTestClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		h.last = req
		return jsonResponse(http.StatusOK, `{"values":[]}`), nil
	}), func(c *config.VLogsConfig) {
		c.ExtraParams = map[string]string{"accountID": "42"}
	})
	_, _ = cl.FieldNames(context.Background(), FieldNamesRequest{Query: "*", Start: time.Now().Add(-time.Hour), End: time.Now()})

	if h.last == nil {
		t.Fatal("no request received")
	}
	// The extra param must be present in the URL; original query params must
	// not be clobbered.
	q := h.last.URL.Query()
	if q.Get("accountID") != "42" {
		t.Errorf("accountID param = %q, want %q", q.Get("accountID"), "42")
	}
	if q.Get("query") == "" {
		t.Error("original 'query' param was clobbered")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// FieldNames / FieldValues tests
// ────────────────────────────────────────────────────────────────────────────

func TestFieldNamesRequest(t *testing.T) {
	cl := newTestClient(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, "/field_names") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.URL.Query().Get("query") == "" {
			t.Error("'query' param missing")
		}
		resp := map[string]any{"values": []map[string]any{
			{"value": "app", "hits": 100},
			{"value": "level", "hits": 50},
			{"value": "host", "hits": 30},
		}}
		body, _ := json.Marshal(resp)
		return jsonResponse(http.StatusOK, string(body)), nil
	}))
	names, err := cl.FieldNames(context.Background(), FieldNamesRequest{
		Query: "*",
		Start: time.Now().Add(-time.Hour),
		End:   time.Now(),
	})
	if err != nil {
		t.Fatalf("FieldNames: %v", err)
	}
	if len(names) != 3 {
		t.Errorf("expected 3 field names, got %d: %v", len(names), names)
	}
	if names[0] != "app" || names[1] != "level" || names[2] != "host" {
		t.Errorf("names = %v, want [app level host]", names)
	}
}

func TestFieldValuesRequest(t *testing.T) {
	cl := newTestClient(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, "/field_values") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.URL.Query().Get("field") == "" {
			t.Error("'field' param missing")
		}
		resp := map[string]any{"values": []map[string]any{
			{"value": "api", "hits": 50},
			{"value": "worker", "hits": 30},
			{"value": "nginx", "hits": 20},
		}}
		body, _ := json.Marshal(resp)
		return jsonResponse(http.StatusOK, string(body)), nil
	}))
	values, err := cl.FieldValues(context.Background(), FieldValuesRequest{
		FieldName: "app",
		Query:     "*",
		Start:     time.Now().Add(-time.Hour),
		End:       time.Now(),
	})
	if err != nil {
		t.Fatalf("FieldValues: %v", err)
	}
	if len(values) != 3 {
		t.Errorf("expected 3 values, got %d: %v", len(values), values)
	}
	if values[0] != "api" || values[1] != "worker" || values[2] != "nginx" {
		t.Errorf("values = %v, want [api worker nginx]", values)
	}
}

func TestFieldNamesRequestNonOKStatus(t *testing.T) {
	cl := newTestClient(roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusInternalServerError, "internal server error\n"), nil
	}))
	_, err := cl.FieldNames(context.Background(), FieldNamesRequest{Query: "*", Start: time.Now().Add(-time.Hour), End: time.Now()})
	if err == nil {
		t.Fatal("expected error for HTTP 500, got nil")
	}
}

func TestQueryLogsRequestAndDecoding(t *testing.T) {
	var gotMethod, gotPath, gotContentType, gotBody string
	cl := newTestClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		gotMethod = req.Method
		gotPath = req.URL.Path
		gotContentType = req.Header.Get("Content-Type")
		body, _ := io.ReadAll(req.Body)
		gotBody = string(body)
		return jsonResponse(http.StatusOK, `{"_msg":"one","_time":"2024-01-15T12:00:00Z"}`+"\n"), nil
	}))

	var records []Record
	err := cl.QueryLogs(context.Background(), LogQueryRequest{
		Query: `app:="api"`,
		Start: time.Unix(1705320000, 0).UTC(),
		End:   time.Unix(1705323600, 0).UTC(),
		Limit: 25,
	}, func(r Record) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("QueryLogs(): %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/select/logsql/query" {
		t.Errorf("request = %s %s, want POST /select/logsql/query", gotMethod, gotPath)
	}
	if gotContentType != "application/x-www-form-urlencoded" {
		t.Errorf("Content-Type = %q", gotContentType)
	}
	if !strings.Contains(gotBody, "query=app%3A%3D%22api%22") || !strings.Contains(gotBody, "limit=25") {
		t.Errorf("encoded body = %q", gotBody)
	}
	if len(records) != 1 || records[0]["_msg"] != "one" {
		t.Errorf("decoded records = %v", records)
	}
}

func TestQueryLogsNonOKStatus(t *testing.T) {
	cl := newTestClient(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusBadGateway, "bad gateway"), nil
	}))

	err := cl.QueryLogs(context.Background(), LogQueryRequest{
		Query: `app:="api"`,
		Start: time.Now().Add(-time.Hour),
		End:   time.Now(),
	}, func(Record) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "VL returned HTTP 502") {
		t.Fatalf("QueryLogs() error = %v, want HTTP status detail", err)
	}
}

func TestQueryHitsSuccessAndErrors(t *testing.T) {
	cl := newTestClient(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"hits":[{"timestamp":"2024-01-15T12:00:00Z","hits":2},{"timestamp":"2024-01-15T12:00:00.123456789Z","hits":3}]}`), nil
	}))

	buckets, err := cl.QueryHits(context.Background(), HitsQueryRequest{
		Query: `app:="api"`,
		Start: time.Now().Add(-time.Hour),
		End:   time.Now(),
		Step:  90 * time.Second,
	})
	if err != nil {
		t.Fatalf("QueryHits(): %v", err)
	}
	if len(buckets) != 2 || buckets[0].Count != 2 || buckets[1].Count != 3 {
		t.Errorf("buckets = %+v", buckets)
	}
	if got := formatDuration(90 * time.Second); got != "1m30s" {
		t.Errorf("formatDuration() = %q, want 1m30s", got)
	}

	bad := newTestClient(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"hits":[{"timestamp":"not-a-time","hits":1}]}`), nil
	}))
	if _, err := bad.QueryHits(context.Background(), HitsQueryRequest{
		Query: `app:="api"`,
		Start: time.Now().Add(-time.Hour),
		End:   time.Now(),
		Step:  time.Minute,
	}); err == nil {
		t.Fatal("expected timestamp parse error, got nil")
	}
}
