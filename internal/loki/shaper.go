package loki

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/fieldclass"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// StreamGrouper accumulates vlogs.Records into Loki streams grouped by their
// label values. It is designed for streaming use: Add is called once per
// record as records arrive from the VictoriaLogs NDJSON decoder.
type StreamGrouper struct {
	knownLabels []string
	enrichment  EnrichmentConfig
	streams     map[string]*streamState
	maxStreams  int
	truncated   bool
}

type streamState struct {
	labels  map[string]string
	values  [][2]string // [ts_ns_string, log_line]
	entries []EnrichedLogEntry
}

// NewStreamGrouper creates a StreamGrouper.
//
// knownLabels is the label allowlist used to build the stream key. Only fields
// whose name appears in knownLabels are included in the stream label set. If
// knownLabels is empty, all non-special fields (_msg, _time) are used.
//
// maxStreams caps the number of distinct streams that may accumulate; records
// that would create a new stream beyond the cap are silently dropped and
// Truncated returns true.
func NewStreamGrouper(knownLabels []string, maxStreams int) *StreamGrouper {
	return NewEnrichedStreamGrouper(knownLabels, EnrichmentConfig{}, maxStreams)
}

// NewEnrichedStreamGrouper creates a StreamGrouper that also classifies each
// incoming VictoriaLogs record into an internal enriched entry model.
func NewEnrichedStreamGrouper(knownLabels []string, enrichment EnrichmentConfig, maxStreams int) *StreamGrouper {
	return &StreamGrouper{
		knownLabels: knownLabels,
		enrichment:  enrichment,
		streams:     make(map[string]*streamState),
		maxStreams:  maxStreams,
	}
}

// Add processes one VL record into the appropriate Loki stream. The method
// satisfies the func(vlogs.Record) error callback signature used by
// VLogsClient.QueryLogs, so it can be passed directly.
func (g *StreamGrouper) Add(rec vlogs.Record) error {
	entry := g.enrichRecord(rec)
	labels := g.extractLabels(rec)
	if g.enrichment.UseIndexedLabelsAsStream {
		labels = cloneMap(entry.IndexedLabels)
	}
	key := buildStreamKey(labels)

	st, ok := g.streams[key]
	if !ok {
		if len(g.streams) >= g.maxStreams {
			g.truncated = true
			return nil
		}
		st = &streamState{labels: labels}
		g.streams[key] = st
	}

	st.values = append(st.values, [2]string{entry.Timestamp, entry.Line})
	st.entries = append(st.entries, entry)
	return nil
}

// Streams returns the accumulated Loki streams. Values within each stream are
// sorted ascending by nanosecond timestamp. The returned slice itself is sorted
// by stream key for deterministic output.
func (g *StreamGrouper) Streams() []LokiStream {
	result := make([]LokiStream, 0, len(g.streams))
	for _, st := range g.streams {
		sort.Slice(st.values, func(i, j int) bool {
			return st.values[i][0] < st.values[j][0]
		})
		result = append(result, LokiStream{
			Stream: st.labels,
			Values: st.values,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return buildStreamKey(result[i].Stream) < buildStreamKey(result[j].Stream)
	})
	return result
}

// CategorizedStreams returns an opt-in richer stream representation used for
// clients that request Loki's categorize-labels response encoding. Each tuple
// is [timestamp, line, metadata], where metadata carries parsed fields and
// structured metadata derived from the enriched entry model.
func (g *StreamGrouper) CategorizedStreams() []CategorizedLokiStream {
	result := make([]CategorizedLokiStream, 0, len(g.streams))
	for _, st := range g.streams {
		sort.Slice(st.entries, func(i, j int) bool {
			return st.entries[i].Timestamp < st.entries[j].Timestamp
		})
		values := make([][]interface{}, 0, len(st.entries))
		for _, entry := range st.entries {
			values = append(values, []interface{}{
				entry.Timestamp,
				entry.Line,
				buildTupleMetadata(entry),
			})
		}
		result = append(result, CategorizedLokiStream{
			Stream: st.labels,
			Values: values,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return buildStreamKey(result[i].Stream) < buildStreamKey(result[j].Stream)
	})
	return result
}

// EnrichedStreams returns the accumulated streams together with the proxy's
// internal classified log entries. This does not alter the external Loki wire
// format; it exists as a foundation for future richer log-detail responses.
func (g *StreamGrouper) EnrichedStreams() []EnrichedLokiStream {
	result := make([]EnrichedLokiStream, 0, len(g.streams))
	for _, st := range g.streams {
		sort.Slice(st.entries, func(i, j int) bool {
			return st.entries[i].Timestamp < st.entries[j].Timestamp
		})
		result = append(result, EnrichedLokiStream{
			Stream:  st.labels,
			Entries: st.entries,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return buildStreamKey(result[i].Stream) < buildStreamKey(result[j].Stream)
	})
	return result
}

// Truncated reports whether any records were dropped because the stream cap
// (maxStreams) was reached. When true, the caller should set the
// X-Proxy-Truncated response header.
func (g *StreamGrouper) Truncated() bool { return g.truncated }

// extractLabels returns a map of only the label fields from rec according to
// the knownLabels allowlist. The _msg and _time fields are always excluded.
func (g *StreamGrouper) extractLabels(rec vlogs.Record) map[string]string {
	out := make(map[string]string)
	if len(g.knownLabels) > 0 {
		for _, k := range g.knownLabels {
			if v, ok := rec[k]; ok {
				out[k] = v
			}
		}
	} else {
		for k, v := range rec {
			if k != "_msg" && k != "_time" {
				out[k] = v
			}
		}
	}
	return out
}

func (g *StreamGrouper) enrichRecord(rec vlogs.Record) EnrichedLogEntry {
	entry := EnrichedLogEntry{
		Timestamp:          parseVLTimestamp(rec["_time"]),
		Line:               rec["_msg"],
		IndexedLabels:      map[string]string{},
		ParsedFields:       map[string]string{},
		StructuredMetadata: map[string]string{},
		OtherFields:        map[string]string{},
	}

	if g.enrichment.UseStreamFieldAsBaseLabels {
		base := parseStreamField(rec["_stream"])
		for k, v := range base {
			entry.IndexedLabels[k] = v
		}
		if len(base) == 0 {
			for _, name := range g.enrichment.Labels.KnownLabels {
				source := name
				if mapped := g.enrichment.Labels.LabelRemap[name]; mapped != "" {
					source = mapped
				}
				if val := rec[source]; val != "" {
					entry.IndexedLabels[name] = val
				}
			}
		}
	}

	for k, v := range rec {
		if k == "_msg" || k == "_time" {
			continue
		}
		switch fieldclass.Classify(k, g.enrichment.Labels) {
		case fieldclass.FieldClassLabel:
			name := fieldclass.DisplayLabelName(k, g.enrichment.Labels)
			if g.enrichment.UseStreamFieldAsBaseLabels {
				// In categorized Drilldown mode, Loki-like indexed labels should
				// primarily come from the original stream selector (_stream) plus
				// synthetic compatibility labels such as detected_level/service_name.
				if name != "service_name" && name != "detected_level" {
					entry.OtherFields[k] = v
					break
				}
			}
			if name == "detected_level" {
				if direct := rec["detected_level"]; direct != "" && k != "detected_level" {
					entry.OtherFields[k] = v
					break
				}
				if k != "detected_level" {
					entry.OtherFields[k] = v
				}
				v = fieldclass.NormalizeDetectedLevel(v)
			}
			entry.IndexedLabels[name] = v
		case fieldclass.FieldClassParsed:
			entry.ParsedFields[k] = v
		case fieldclass.FieldClassStructuredMetadata:
			entry.StructuredMetadata[k] = v
		case fieldclass.FieldClassExcluded:
			// Do not expose excluded fields in enriched output.
		default:
			entry.OtherFields[k] = v
		}
	}
	if fieldclass.IsKnownLabel("service_name", g.enrichment.Labels) && entry.IndexedLabels["service_name"] == "" {
		if val := syntheticServiceName(rec); val != "" {
			entry.IndexedLabels["service_name"] = val
		}
	}

	return entry
}

func buildTupleMetadata(entry EnrichedLogEntry) map[string]map[string]string {
	meta := make(map[string]map[string]string)
	parsed := make(map[string]string, len(entry.ParsedFields)+len(entry.StructuredMetadata)+len(entry.OtherFields))
	for k, v := range entry.ParsedFields {
		parsed[k] = v
	}
	for k, v := range entry.StructuredMetadata {
		parsed[k] = v
	}
	for k, v := range entry.OtherFields {
		parsed[k] = v
	}
	if len(parsed) > 0 {
		meta["parsed"] = parsed
	}
	return meta
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func syntheticServiceName(rec vlogs.Record) string {
	for _, field := range []string{
		"service_name",
		"service.name",
		"service",
		"labels.app.kubernetes.io/name",
		"labels.k8s-app",
		"labels.app",
		"app",
		"application",
		"app_name",
		"app_kubernetes_io_name",
		"container",
		"k8s.container.name",
		"container.name",
		"container_name",
		"k8s_container_name",
		"job",
		"k8s.job.name",
		"k8s_job_name",
	} {
		if val := rec[field]; val != "" {
			return val
		}
	}
	return ""
}

func parseStreamField(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return map[string]string{}
	}
	raw = strings.TrimPrefix(raw, "{")
	raw = strings.TrimSuffix(raw, "}")
	out := make(map[string]string)
	start := 0
	inQuotes := false
	escape := false
	parts := make([]string, 0, 4)
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if escape {
			escape = false
			continue
		}
		if ch == '\\' && inQuotes {
			escape = true
			continue
		}
		if ch == '"' {
			inQuotes = !inQuotes
			continue
		}
		if ch == ',' && !inQuotes {
			parts = append(parts, raw[start:i])
			start = i + 1
		}
	}
	parts = append(parts, raw[start:])
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.Index(part, "=")
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(part[:eq])
		val := strings.TrimSpace(part[eq+1:])
		val = strings.Trim(val, `"`)
		val = strings.ReplaceAll(val, `\"`, `"`)
		out[key] = val
	}
	return out
}

// buildStreamKey returns a canonical JSON-like string representation of the
// label map, used as the grouping key. Keys are sorted for stability.
func buildStreamKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%q:%q", k, labels[k])
	}
	sb.WriteByte('}')
	return sb.String()
}

// parseVLTimestamp converts a VictoriaLogs _time field (RFC3339Nano) to the
// nanosecond Unix timestamp decimal string that Loki uses in its values arrays.
func parseVLTimestamp(s string) string {
	if s == "" {
		return "0"
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return "0"
		}
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}

// ShapeMatrix converts a slice of VL hit buckets into a Loki matrix result
// slice. metric is the label set associated with the series (typically the
// equality matchers extracted from the original LogQL query).
//
// When isRate is true the bucket count is divided by stepSec to produce a
// per-second rate value, matching the semantics of LogQL rate().
func ShapeMatrix(buckets []vlogs.HitBucket, metric map[string]string, isRate bool, stepSec float64) []MatrixSeries {
	values := make([][]interface{}, 0, len(buckets))
	for _, b := range buckets {
		// Matrix timestamps are Unix seconds as a float (not nanoseconds).
		ts := float64(b.Timestamp.UnixNano()) / 1e9
		var valStr string
		if isRate && stepSec > 0 {
			valStr = strconv.FormatFloat(float64(b.Count)/stepSec, 'f', -1, 64)
		} else {
			valStr = strconv.FormatInt(b.Count, 10)
		}
		values = append(values, []interface{}{ts, valStr})
	}
	if metric == nil {
		metric = map[string]string{}
	}
	return []MatrixSeries{{
		Metric: metric,
		Values: values,
	}}
}
