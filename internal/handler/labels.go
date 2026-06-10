package handler

import (
	"log/slog"
	"strings"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// Labels handles GET /loki/api/v1/labels.
//
// Returns the list of indexed field names from VictoriaLogs, which Grafana
// uses to populate the label selector. Results are cached per time-range
// bucket (rounded to the nearest minute) to reduce VL load during the
// frequent polls that Grafana Logs Drilldown performs.
//
// If Labels.KnownLabels is set in config, the static list is returned
// immediately without querying VictoriaLogs or the cache.
func (d *Deps) Labels(ctx *fasthttp.RequestCtx) {
	start, end, err := parseTimeRange(ctx)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	// Static allowlist fast path.
	if len(d.Cfg.Labels.KnownLabels) > 0 {
		names := filterNames(d.Cfg.Labels.KnownLabels, d.Cfg.Labels.AllowLabels, d.Cfg.Labels.DenyLabels)
		writeJSON(ctx, fasthttp.StatusOK, loki.LabelsResponse{
			Status: "success",
			Data:   names,
		})
		return
	}

	// Cache lookup.
	key := vlogs.FieldNamesKey(start, end)
	if cached, ok := d.Cache.Get(key); ok {
		writeJSON(ctx, fasthttp.StatusOK, loki.LabelsResponse{Status: "success", Data: cached})
		return
	}

	names, err := d.VL.FieldNames(reqContext(ctx), vlogs.FieldNamesRequest{
		Query: "*",
		Start: start,
		End:   end,
	})
	if err != nil {
		slog.Error("FieldNames failed",
			"query", "*",
			"start", start,
			"end", end,
			"err", err,
		)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"VictoriaLogs field_names query failed: "+err.Error())
		return
	}

	if d.Cfg.Labels.MetadataCacheTTL > 0 {
		names = filterNames(names, d.Cfg.Labels.AllowLabels, d.Cfg.Labels.DenyLabels)
		d.Cache.Set(key, names, d.Cfg.Labels.MetadataCacheTTL)
	}

	names = filterNames(names, d.Cfg.Labels.AllowLabels, d.Cfg.Labels.DenyLabels)
	writeJSON(ctx, fasthttp.StatusOK, loki.LabelsResponse{Status: "success", Data: names})
}

// DetectedLabels handles GET /loki/api/v1/detected_labels.
//
// Grafana Logs Drilldown calls this endpoint (instead of /labels) to discover
// which label names are present in the selected time range. The Loki response
// shape is different — it wraps names in a "detectedLabels" array with a
// per-label cardinality count — but the underlying data source is the same VL
// field_names query used by /labels.
//
// VictoriaLogs does not expose per-label cardinality, so every entry reports
// cardinality 0. Grafana Logs Drilldown only uses the label names, so this is
// functionally equivalent.
func (d *Deps) DetectedLabels(ctx *fasthttp.RequestCtx) {
	start, end, err := parseTimeRange(ctx)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	// Static allowlist fast path.
	if len(d.Cfg.Labels.KnownLabels) > 0 {
		names := filterNames(d.Cfg.Labels.KnownLabels, d.Cfg.Labels.AllowLabels, d.Cfg.Labels.DenyLabels)
		dl := make([]loki.DetectedLabel, len(names))
		for i, name := range names {
			dl[i] = loki.DetectedLabel{Label: name}
		}
		writeJSON(ctx, fasthttp.StatusOK, loki.DetectedLabelsData{DetectedLabels: dl})
		return
	}

	// Cache lookup (shared key with /labels — same underlying VL query).
	key := vlogs.FieldNamesKey(start, end)
	if cached, ok := d.Cache.Get(key); ok {
		writeJSON(ctx, fasthttp.StatusOK, loki.DetectedLabelsData{DetectedLabels: toDetectedLabels(cached)})
		return
	}

	names, err := d.VL.FieldNames(reqContext(ctx), vlogs.FieldNamesRequest{
		Query: "*",
		Start: start,
		End:   end,
	})
	if err != nil {
		slog.Error("FieldNames failed (detected_labels)",
			"query", "*",
			"start", start,
			"end", end,
			"err", err,
		)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"VictoriaLogs field_names query failed: "+err.Error())
		return
	}

	if d.Cfg.Labels.MetadataCacheTTL > 0 {
		names = filterNames(names, d.Cfg.Labels.AllowLabels, d.Cfg.Labels.DenyLabels)
		d.Cache.Set(key, names, d.Cfg.Labels.MetadataCacheTTL)
	}

	names = filterNames(names, d.Cfg.Labels.AllowLabels, d.Cfg.Labels.DenyLabels)
	writeJSON(ctx, fasthttp.StatusOK, loki.DetectedLabelsData{DetectedLabels: toDetectedLabels(names)})
}

// toDetectedLabels converts a slice of label name strings into the
// DetectedLabel slice required by the /detected_labels response format.
func toDetectedLabels(names []string) []loki.DetectedLabel {
	out := make([]loki.DetectedLabel, len(names))
	for i, n := range names {
		out[i] = loki.DetectedLabel{Label: n}
	}
	return out
}

// DetectedFields handles GET /loki/api/v1/detected_fields.
//
// Grafana Logs Drilldown calls this endpoint to discover the set of field names
// (with their types and cardinalities) present in the selected log streams.
// It is backed by VictoriaLogs' GET /select/logsql/field_names endpoint.
//
// The query may contain parser and filter pipeline stages such as `| json`,
// `| logfmt`, and `| drop`. For field discovery we only need the stream
// selector scoping; including the pipeline can surface transient extracted
// tokens from _msg (for example via logfmt unpacking) as if they were stable
// fields. This handler therefore uses only the selector portion of the query.
//
// VictoriaLogs does not expose per-field type or cardinality; every field is
// returned with type="string" and cardinality=0, which is sufficient for
// Grafana Logs Drilldown to populate its field picker.
func (d *Deps) DetectedFields(ctx *fasthttp.RequestCtx) {
	start, end, err := parseTimeRange(ctx)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	queryStr := string(ctx.QueryArgs().Peek("query"))
	logsqlFilter := selectorOnlyLogsQLFilter(queryStr, translator.Options{
		LabelRemap:                d.Cfg.Labels.LabelRemap,
		ServiceNameFallbackFields: d.Cfg.Labels.ServiceNameFallbackFields,
	})

	names, err := d.VL.FieldNames(reqContext(ctx), vlogs.FieldNamesRequest{
		Query: logsqlFilter,
		Start: start,
		End:   end,
	})
	if err != nil {
		slog.Error("FieldNames failed (detected_fields)",
			"query", logsqlFilter,
			"start", start,
			"end", end,
			"err", err,
		)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"VictoriaLogs field_names query failed: "+err.Error())
		return
	}

	names = filterNames(names, d.Cfg.Labels.AllowFields, d.Cfg.Labels.DenyFields)
	fields := make([]loki.DetectedField, len(names))
	for i, name := range names {
		fields[i] = loki.DetectedField{Label: name, Type: "string"}
	}

	writeJSON(ctx, fasthttp.StatusOK, loki.DetectedFieldsResponse{Fields: fields})
}

func filterNames(names, allow, deny []string) []string {
	if len(names) == 0 {
		return nil
	}
	allowSet := make(map[string]struct{}, len(allow))
	for _, name := range allow {
		allowSet[name] = struct{}{}
	}
	denySet := make(map[string]struct{}, len(deny))
	for _, name := range deny {
		denySet[name] = struct{}{}
	}

	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if len(allowSet) > 0 {
			if _, ok := allowSet[name]; !ok {
				continue
			}
		}
		if _, denied := denySet[name]; denied {
			continue
		}
		filtered = append(filtered, name)
	}
	return filtered
}

func selectorOnlyLogsQLFilter(queryStr string, opts translator.Options) string {
	if queryStr == "" {
		return "*"
	}
	selector := extractStreamSelector(queryStr)
	if selector == "" {
		return "*"
	}
	if ast, err := parser.Parse(selector); err == nil {
		if result, err := translator.Translate(ast, opts); err == nil {
			return result.LogsQL
		}
	}
	return "*"
}

// bestEffortLogsQLFilter converts a LogQL query string to a LogsQL filter on a
// best-effort basis. It is intended for metadata endpoints (detected_fields,
// detected_labels) where only the stream selector matters for scoping.
//
// Strategy:
//  1. Try to parse and translate the full query.
//  2. On failure (unsupported pipeline stage like `| drop`, `| unpack_json`,
//     etc.) strip the pipeline and retry with just the stream selector `{...}`.
//  3. If that also fails, return "*" (match-all).
func bestEffortLogsQLFilter(queryStr string, opts translator.Options) string {
	if queryStr == "" {
		return "*"
	}

	// Attempt 1: full query.
	if ast, err := parser.Parse(queryStr); err == nil {
		if result, err := translator.Translate(ast, opts); err == nil {
			return result.LogsQL
		}
	}

	// Attempt 2: strip the pipeline by keeping only the stream selector.
	// Find the closing brace of the outermost `{...}` block and truncate there.
	selector := extractStreamSelector(queryStr)
	if selector != "" {
		if ast, err := parser.Parse(selector); err == nil {
			if result, err := translator.Translate(ast, opts); err == nil {
				return result.LogsQL
			}
		}
	}

	return "*"
}

// extractStreamSelector returns the first `{...}` token from s, including the
// braces. It handles nested braces correctly. Returns "" if none is found.
func extractStreamSelector(s string) string {
	start := strings.IndexByte(s, '{')
	if start < 0 {
		return ""
	}
	depth := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return ""
}
