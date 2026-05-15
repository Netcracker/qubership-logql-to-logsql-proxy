package translator_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
)

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

// parseAndTranslate parses a LogQL string and translates it, failing the test
// on any error.
func parseAndTranslate(t *testing.T, query string) translator.Result {
	t.Helper()
	q, err := parser.Parse(query)
	if err != nil {
		t.Fatalf("Parse(%q): %v", query, err)
	}
	r, err := translator.Translate(q, translator.Options{})
	if err != nil {
		t.Fatalf("Translate(%q): %v", query, err)
	}
	return r
}

// ────────────────────────────────────────────────────────────────────────────
// Success cases
// ────────────────────────────────────────────────────────────────────────────

func TestTranslateEmptySelector(t *testing.T) {
	r := parseAndTranslate(t, `{}`)
	if r.LogsQL != "*" {
		t.Errorf("got %q, want %q", r.LogsQL, "*")
	}
	if r.IsMetric || r.HasJSONParser {
		t.Errorf("unexpected flags: IsMetric=%v HasJSONParser=%v", r.IsMetric, r.HasJSONParser)
	}
}

func TestTranslateSimpleSelector(t *testing.T) {
	r := parseAndTranslate(t, `{app="api"}`)
	want := `app:="api"`
	if r.LogsQL != want {
		t.Errorf("got %q, want %q", r.LogsQL, want)
	}
}

func TestTranslateSyntheticServiceNameRegexMatcher(t *testing.T) {
	r := parseAndTranslate(t, `{service_name=~".+"}`)
	for _, want := range []string{
		`service_name:~".+"`,
		`"labels.app.kubernetes.io/name":~".+"`,
		`container:~".+"`,
	} {
		if !strings.Contains(r.LogsQL, want) {
			t.Errorf("LogsQL %q does not contain %q", r.LogsQL, want)
		}
	}
}

func TestTranslateSyntheticServiceNameNotEmptyMatcher(t *testing.T) {
	r := parseAndTranslate(t, `{service_name!=""}`)
	for _, want := range []string{
		`service_name:~".+"`,
		`"labels.app.kubernetes.io/name":~".+"`,
		`container:~".+"`,
	} {
		if !strings.Contains(r.LogsQL, want) {
			t.Errorf("LogsQL %q does not contain %q", r.LogsQL, want)
		}
	}
	if strings.Contains(r.LogsQL, `NOT service_name:=""`) {
		t.Errorf("LogsQL %q still uses the old empty-negation form", r.LogsQL)
	}
}

func TestTranslateInternalStreamSelector(t *testing.T) {
	r := parseAndTranslate(t, `{_stream="{container=\"cloud-provider-kind\",namespace=\"kube-system\"}"}`)
	want := `{container="cloud-provider-kind",namespace="kube-system"}`
	if r.LogsQL != want {
		t.Errorf("got %q, want %q", r.LogsQL, want)
	}
}

func TestTranslateInternalStreamSelectorNotEmptyIsDropped(t *testing.T) {
	r := parseAndTranslate(t, `{level="info", _stream!=""}`)
	want := `level:="info"`
	if r.LogsQL != want {
		t.Errorf("got %q, want %q", r.LogsQL, want)
	}
}

func TestTranslateInternalStreamIDSelectorNotEmptyIsDropped(t *testing.T) {
	r := parseAndTranslate(t, `{level="info", _stream_id!=""}`)
	want := `level:="info"`
	if r.LogsQL != want {
		t.Errorf("got %q, want %q", r.LogsQL, want)
	}
}

func TestTranslateInternalTimeSelectorNotEmptyIsDropped(t *testing.T) {
	r := parseAndTranslate(t, `{level="err", _time!=""}`)
	want := `level:="err"`
	if r.LogsQL != want {
		t.Errorf("got %q, want %q", r.LogsQL, want)
	}
}

// TestTranslateMultipleFilters covers the canonical combined example from the PLAN:
//
//	{app="api", level!="debug"} |= "error"
//	→ app:="api" AND NOT level:="debug" AND _msg:"error"
func TestTranslateMultipleFilters(t *testing.T) {
	r := parseAndTranslate(t, `{app="api", level!="debug"} |= "error"`)
	want := `app:="api" AND NOT level:="debug" AND _msg:"error"`
	if r.LogsQL != want {
		t.Errorf("got  %q\nwant %q", r.LogsQL, want)
	}
}

func TestTranslateRegexMatcher(t *testing.T) {
	r := parseAndTranslate(t, `{app=~"api.*", ns!~"test.*"}`)
	want := `app:~"api.*" AND NOT ns:~"test.*"`
	if r.LogsQL != want {
		t.Errorf("got %q, want %q", r.LogsQL, want)
	}
}

func TestTranslateLineFilter(t *testing.T) {
	r := parseAndTranslate(t, `{app="api"} |= "error"`)
	want := `app:="api" AND _msg:"error"`
	if r.LogsQL != want {
		t.Errorf("got %q, want %q", r.LogsQL, want)
	}
}

func TestTranslatePatternFilter(t *testing.T) {
	r := parseAndTranslate(t, `{level="warning"} |> "addr <IP4>:<N>"`)
	for _, want := range []string{
		`level:="warning"`,
		`_msg:~"addr[[:space:]]+`,
		`(?:[0-9]{1,3}\\.){3}[0-9]{1,3}`,
		`[0-9]+`,
	} {
		if !strings.Contains(r.LogsQL, want) {
			t.Errorf("LogsQL %q does not contain %q", r.LogsQL, want)
		}
	}
}

func TestTranslatePatternFilterWhitespaceIsFlexible(t *testing.T) {
	r := parseAndTranslate(t, "{service_name=\"grafana-operator\"} |> `<N>.<N>+<N>\tINFO\trunning periodic dashboardfolder resync`")
	for _, want := range []string{
		`service_name`,
		`[[:space:]]+INFO[[:space:]]+running[[:space:]]+periodic[[:space:]]+dashboardfolder[[:space:]]+resync`,
	} {
		if !strings.Contains(r.LogsQL, want) {
			t.Errorf("LogsQL %q does not contain %q", r.LogsQL, want)
		}
	}
}

func TestTranslatePatternFilterMatchesScientificNotation(t *testing.T) {
	r := parseAndTranslate(t, "{service_name=\"grafana-operator\"} |> `<N>.<N>+<N>\tINFO\trunning periodic dashboard resync`")
	if !strings.Contains(r.LogsQL, `(?:e|E)?(?:\\+|%20|[[:space:]]+)[0-9]+`) {
		t.Errorf("LogsQL %q does not contain scientific-notation matcher", r.LogsQL)
	}
}

func TestTranslatePatternFilterMatchesPercentEncodedHexFragments(t *testing.T) {
	r := parseAndTranslate(t, "{service_name=\"vmalert\"} |> `%<N> query=%7Bjob%3D%22etcd%22%7D`")
	if !strings.Contains(r.LogsQL, `%[0-9A-Fa-f]+`) {
		t.Errorf("LogsQL %q does not contain percent-encoded hex matcher", r.LogsQL)
	}
}

func TestTranslateEmptyLineFilterIsNoop(t *testing.T) {
	r := parseAndTranslate(t, "{app=`api`} |= ``")
	want := `app:="api"`
	if r.LogsQL != want {
		t.Errorf("got %q, want %q", r.LogsQL, want)
	}
}

// TestTranslateNotLineFilter covers pipeline `!= "text"` (not-contains) and
// `!~ "re"` (not-regex).
func TestTranslateNotLineFilter(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{`{app="api"} != "debug"`, `app:="api" AND NOT _msg:"debug"`},
		{`{app="api"} !~ "err.*"`, `app:="api" AND NOT _msg:~"err.*"`},
	}
	for _, tc := range tests {
		r := parseAndTranslate(t, tc.query)
		if r.LogsQL != tc.want {
			t.Errorf("query %q\n  got  %q\n  want %q", tc.query, r.LogsQL, tc.want)
		}
	}
}

func TestTranslateCountOverTime(t *testing.T) {
	r := parseAndTranslate(t, `count_over_time({app="api"}[5m])`)

	if !r.IsMetric {
		t.Fatal("expected IsMetric=true")
	}
	if r.MetricFunc != parser.CountOverTime {
		t.Errorf("expected CountOverTime, got %d", r.MetricFunc)
	}
	if r.MetricRange != 5*time.Minute {
		t.Errorf("expected range 5m, got %v", r.MetricRange)
	}
	if want := `app:="api"`; r.LogsQL != want {
		t.Errorf("LogsQL: got %q, want %q", r.LogsQL, want)
	}
}

func TestTranslateRate(t *testing.T) {
	r := parseAndTranslate(t, `rate({app="api", level!="debug"}[1h])`)

	if !r.IsMetric {
		t.Fatal("expected IsMetric=true")
	}
	if r.MetricFunc != parser.Rate {
		t.Errorf("expected Rate, got %d", r.MetricFunc)
	}
	if r.MetricRange != time.Hour {
		t.Errorf("expected range 1h, got %v", r.MetricRange)
	}
	if want := `app:="api" AND NOT level:="debug"`; r.LogsQL != want {
		t.Errorf("LogsQL: got %q, want %q", r.LogsQL, want)
	}
}

// TestTranslateJSONStage checks that | json sets HasJSONParser but does not
// add any text to the LogsQL query string.
func TestTranslateJSONStage(t *testing.T) {
	r := parseAndTranslate(t, `{app="api"} | json`)
	if !r.HasJSONParser {
		t.Error("expected HasJSONParser=true")
	}
	want := `app:="api"`
	if r.LogsQL != want {
		t.Errorf("LogsQL: got %q, want %q", r.LogsQL, want)
	}
}

// TestTranslateLogfmtStage checks that | logfmt is translated to | unpack_logfmt.
func TestTranslateLogfmtStage(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{`{app="api"} | logfmt`, `app:="api" | unpack_logfmt`},
		{`{} | logfmt`, `* | unpack_logfmt`},
		{`{app="api"} |= "error" | logfmt`, `app:="api" AND _msg:"error" | unpack_logfmt`},
		{`{app="api"} | json | logfmt`, `app:="api"`},
	}
	for _, tc := range tests {
		r := parseAndTranslate(t, tc.query)
		if r.LogsQL != tc.want {
			t.Errorf("query %q\n  got  %q\n  want %q", tc.query, r.LogsQL, tc.want)
		}
	}
}

// TestTranslateEscaping verifies that literal values with special characters
// are correctly escaped in the LogsQL output.
func TestTranslateEscaping(t *testing.T) {
	// Label value containing a double-quote (already unescaped by the parser)
	r := parseAndTranslate(t, `{app="my \"service\""}`)
	want := `app:="my \"service\""`
	if r.LogsQL != want {
		t.Errorf("got %q, want %q", r.LogsQL, want)
	}
}

func TestTranslateSumByCountOverTime(t *testing.T) {
	r := parseAndTranslate(t, "sum by (detected_level) (count_over_time({namespace=`pyroscope`}[2s]))")

	if !r.IsMetric {
		t.Fatal("expected IsMetric=true")
	}
	if !r.IsAggregation {
		t.Fatal("expected IsAggregation=true")
	}
	if r.AggregationFunc != parser.AggSum {
		t.Errorf("AggregationFunc = %d, want AggSum", r.AggregationFunc)
	}
	if len(r.AggregateBy) != 1 || r.AggregateBy[0] != "detected_level" {
		t.Errorf("AggregateBy = %v, want [detected_level]", r.AggregateBy)
	}
	if r.MetricFunc != parser.CountOverTime {
		t.Errorf("MetricFunc = %d, want CountOverTime", r.MetricFunc)
	}
	if r.MetricRange != 2*time.Second {
		t.Errorf("MetricRange = %v, want 2s", r.MetricRange)
	}
	want := `namespace:="pyroscope"`
	if r.LogsQL != want {
		t.Errorf("LogsQL: got %q, want %q", r.LogsQL, want)
	}
}

func TestTranslateSumNoBy(t *testing.T) {
	r := parseAndTranslate(t, `sum(count_over_time({app="api"}[5m]))`)

	if !r.IsMetric || !r.IsAggregation {
		t.Fatalf("expected IsMetric=true IsAggregation=true, got IsMetric=%v IsAggregation=%v",
			r.IsMetric, r.IsAggregation)
	}
	if len(r.AggregateBy) != 0 {
		t.Errorf("AggregateBy = %v, want empty (no grouping)", r.AggregateBy)
	}
	if r.LogsQL != `app:="api"` {
		t.Errorf("LogsQL: got %q, want %q", r.LogsQL, `app:="api"`)
	}
}

func TestTranslateAggregationWithInternalStreamNotEmpty(t *testing.T) {
	r := parseAndTranslate(t, `sum(count_over_time({level="info", _stream!=""}[2s])) by (_stream)`)
	if !r.IsAggregation {
		t.Fatal("expected IsAggregation=true")
	}
	if r.LogsQL != `level:="info"` {
		t.Errorf("LogsQL: got %q, want %q", r.LogsQL, `level:="info"`)
	}
	if len(r.AggregateBy) != 1 || r.AggregateBy[0] != "_stream" {
		t.Errorf("AggregateBy = %v, want [_stream]", r.AggregateBy)
	}
}

func TestTranslateAggregationWithInternalStreamIDNotEmpty(t *testing.T) {
	r := parseAndTranslate(t, `sum(count_over_time({level="info", _stream_id!=""}[2s])) by (_stream_id)`)
	if !r.IsAggregation {
		t.Fatal("expected IsAggregation=true")
	}
	if r.LogsQL != `level:="info"` {
		t.Errorf("LogsQL: got %q, want %q", r.LogsQL, `level:="info"`)
	}
	if len(r.AggregateBy) != 1 || r.AggregateBy[0] != "_stream_id" {
		t.Errorf("AggregateBy = %v, want [_stream_id]", r.AggregateBy)
	}
}

func TestTranslateAggregationWithInternalTimeNotEmpty(t *testing.T) {
	r := parseAndTranslate(t, `sum(count_over_time({level="err", _time!=""}[500ms])) by (_time)`)
	if !r.IsAggregation {
		t.Fatal("expected IsAggregation=true")
	}
	if r.LogsQL != `level:="err"` {
		t.Errorf("LogsQL: got %q, want %q", r.LogsQL, `level:="err"`)
	}
	if len(r.AggregateBy) != 1 || r.AggregateBy[0] != "_time" {
		t.Errorf("AggregateBy = %v, want [_time]", r.AggregateBy)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Error case
// ────────────────────────────────────────────────────────────────────────────

// TestTranslateUnsupportedReturnsError verifies that the parse→translate
// pipeline surfaces a typed error for LogQL constructs that are valid syntax
// but are not supported by this proxy.
//
// Note: parser.Query is a sealed interface (unexported method), so external
// packages cannot create unknown implementations. The "unsupported" error
// therefore originates from the parser (*UnsupportedError), and this test
// validates that the pipeline correctly surfaces it — the caller must never
// call Translate with a query that the parser rejected.
func TestTranslateUnsupportedReturnsError(t *testing.T) {
	unsupportedQueries := []string{
		`{app="api"} | label_format app=svc`,
	}
	for _, q := range unsupportedQueries {
		_, err := parser.Parse(q)
		if err == nil {
			t.Errorf("Parse(%q): expected error, got nil", q)
			continue
		}
		var unsupportedErr *parser.UnsupportedError
		if !errors.As(err, &unsupportedErr) {
			t.Errorf("Parse(%q): expected *UnsupportedError, got %T: %v", q, err, err)
		}
	}
}
