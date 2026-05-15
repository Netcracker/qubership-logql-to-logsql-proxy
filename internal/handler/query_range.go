package handler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

var drilldownPatternStatsRe = regexp.MustCompile("(?i)\\|\\s*pattern\\s*([`\"].*?[`\"])" +
	"\\s*\\|\\s*keep\\s+([A-Za-z0-9_./-]+)" +
	"\\s*\\|\\s*line_format\\s*\"\"")

// QueryRange handles GET /loki/api/v1/query_range.
//
// This is the primary endpoint Grafana Logs Drilldown calls. It accepts a LogQL
// query over a time range, translates it to LogsQL, dispatches to VictoriaLogs,
// and shapes the response into the Loki streams or matrix JSON format.
func (d *Deps) QueryRange(ctx *fasthttp.RequestCtx) {
	d.handleQuery(ctx, false)
}

// Query handles GET /loki/api/v1/query (instant query).
//
// Same pipeline as QueryRange; the only difference is that Grafana omits the
// `step` parameter and uses a single `time` point instead of start/end. We
// treat `time` as both start and end (with a 1-second window) so the
// underlying logic is identical.
func (d *Deps) Query(ctx *fasthttp.RequestCtx) {
	d.handleQuery(ctx, true)
}

// ────────────────────────────────────────────────────────────────────────────
// Shared implementation
// ────────────────────────────────────────────────────────────────────────────

func (d *Deps) handleQuery(ctx *fasthttp.RequestCtx, instant bool) {
	queryStr := string(ctx.QueryArgs().Peek("query"))
	if queryStr == "" {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", "query parameter is required")
		return
	}

	// Grafana sends "vector(1)+vector(1)" (a PromQL expression) when testing a
	// Loki datasource connection. It is not valid LogQL; return a minimal
	// success response so the datasource health check passes.
	if isVectorExpr(queryStr) {
		writeJSON(ctx, fasthttp.StatusOK, loki.VectorResponse{
			Status: "success",
			Data: loki.VectorData{
				ResultType: "vector",
				Result: []loki.VectorSample{
					{
						Metric: map[string]string{},
						Value:  []interface{}{float64(time.Now().Unix()), "2"},
					},
				},
			},
		})
		return
	}

	var (
		start time.Time
		end   time.Time
		err   error
	)

	if instant {
		// Instant query: use "time" param, fall back to now.
		tStr := string(ctx.QueryArgs().Peek("time"))
		if tStr == "" {
			end = time.Now()
		} else {
			end, err = parseTime(tStr)
			if err != nil {
				writeError(ctx, fasthttp.StatusBadRequest, "bad_data",
					fmt.Sprintf("invalid time: %v", err))
				return
			}
		}
		start = end.Add(-time.Second)
	} else {
		start, end, err = parseTimeRange(ctx)
		if err != nil {
			writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
			return
		}
	}

	// Reject queries that span more than MaxQueryRangeHours.
	if end.Sub(start) > time.Duration(d.Cfg.Limits.MaxQueryRangeHours)*time.Hour {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data",
			fmt.Sprintf("time range exceeds maximum of %d hours", d.Cfg.Limits.MaxQueryRangeHours))
		return
	}

	// Parse limit (log queries only; ignored for metric queries).
	limit := d.Cfg.Limits.DefaultLimit
	if limStr := string(ctx.QueryArgs().Peek("limit")); limStr != "" {
		if n, err := strconv.Atoi(limStr); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > d.Cfg.Limits.MaxLimit {
		limit = d.Cfg.Limits.MaxLimit
	}

	// Parse step (required for metric queries; ignored for log queries).
	var step time.Duration
	if stepStr := string(ctx.QueryArgs().Peek("step")); stepStr != "" {
		step = parseDuration(stepStr)
	}

	// Parse LogQL.
	ast, err := parser.Parse(queryStr)
	if err != nil {
		var unsup *parser.UnsupportedError
		if errors.As(err, &unsup) {
			writeError(ctx, fasthttp.StatusBadRequest, "bad_data",
				"unsupported LogQL construct: "+unsup.Construct)
		} else {
			writeError(ctx, fasthttp.StatusBadRequest, "bad_data",
				"invalid LogQL query: "+err.Error())
		}
		return
	}

	// Translate to LogsQL.
	result, err := translator.Translate(ast, translator.Options{LabelRemap: d.Cfg.Labels.LabelRemap})
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}
	if extractPattern, keepField, ok := extractDrilldownPatternStatsStage(queryStr); ok && !result.IsMetric && !result.IsAggregation {
		result.LogsQL = augmentPatternStatsLogsQL(result.LogsQL, extractPattern, keepField)
	}

	switch {
	case result.IsAggregation:
		d.handleAggregationQuery(ctx, result, start, end, step)
	case result.IsMetric:
		d.handleMetricQuery(ctx, result, ast, start, end, step)
	default:
		d.handleLogQuery(ctx, result, start, end, limit)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Log query path
// ────────────────────────────────────────────────────────────────────────────

func (d *Deps) handleLogQuery(
	ctx *fasthttp.RequestCtx,
	result translator.Result,
	start, end time.Time,
	limit int,
) {
	categorizedLabels := wantsCategorizedLabels(ctx)
	grouper := loki.NewEnrichedStreamGrouper(
		nil, // keep full record fields in log results; KnownLabels is for discovery endpoints only
		loki.EnrichmentConfig{
			Labels:                     d.Cfg.Labels,
			UseIndexedLabelsAsStream:   true,
			UseStreamFieldAsBaseLabels: true,
		},
		d.Cfg.Limits.MaxStreamsPerResponse,
	)

	err := d.VL.QueryLogs(reqContext(ctx), vlogs.LogQueryRequest{
		Query: result.LogsQL,
		Start: start,
		End:   end,
		Limit: limit,
	}, grouper.Add)

	switch {
	case err == nil:
		// full result
	case errors.Is(err, vlogs.ErrResponseTooLarge):
		ctx.Response.Header.Set("X-Proxy-Truncated", "true")
		slog.Warn("QueryLogs response truncated: body size limit reached",
			"logsql", result.LogsQL)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(ctx, fasthttp.StatusGatewayTimeout, "timeout", "query timed out")
		return
	default:
		slog.Error("QueryLogs failed", "logsql", result.LogsQL, "err", err)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"VictoriaLogs query failed: "+err.Error())
		return
	}

	if grouper.Truncated() {
		ctx.Response.Header.Set("X-Proxy-Truncated", "true")
	}

	if categorizedLabels {
		writeJSON(ctx, fasthttp.StatusOK, loki.CategorizedStreamsResponse{
			Status: "success",
			Data: loki.CategorizedStreamsData{
				ResultType:    "streams",
				EncodingFlags: []string{"categorize-labels"},
				Result:        grouper.CategorizedStreams(),
			},
		})
		return
	}

	writeJSON(ctx, fasthttp.StatusOK, loki.StreamsResponse{
		Status: "success",
		Data: loki.StreamsData{
			ResultType: "streams",
			Result:     grouper.Streams(),
		},
	})
}

func wantsCategorizedLabels(ctx *fasthttp.RequestCtx) bool {
	raw := strings.ToLower(strings.TrimSpace(string(ctx.Request.Header.Peek("X-Loki-Response-Encoding-Flags"))))
	if raw != "" {
		for _, part := range strings.Split(raw, ",") {
			if strings.TrimSpace(part) == "categorize-labels" {
				return true
			}
		}
	}
	// Grafana Logs Drilldown does not always forward the optional Loki response
	// encoding header through every datasource path. When the query clearly
	// matches its "log details" shape, return categorized tuples anyway so the
	// UI can populate Parsed fields / Indexed labels consistently.
	query := string(ctx.QueryArgs().Peek("query"))
	return looksLikeDrilldownDetailsQuery(query) || looksLikeDrilldownPatternStatsQuery(query)
}

func looksLikeDrilldownDetailsQuery(query string) bool {
	if query == "" {
		return false
	}
	normalized := strings.ToLower(strings.Join(strings.Fields(query), " "))
	return strings.Contains(normalized, "| json") &&
		strings.Contains(normalized, "| logfmt") &&
		strings.Contains(normalized, "drop __error__, __error_details__")
}

func looksLikeDrilldownPatternStatsQuery(query string) bool {
	if query == "" {
		return false
	}
	normalized := strings.ToLower(strings.Join(strings.Fields(query), " "))
	return strings.Contains(normalized, "| pattern") &&
		strings.Contains(normalized, "| keep") &&
		strings.Contains(normalized, `| line_format ""`)
}

func extractDrilldownPatternStatsStage(query string) (extractPattern, keepField string, ok bool) {
	matches := drilldownPatternStatsRe.FindStringSubmatch(query)
	if len(matches) != 3 {
		return "", "", false
	}
	rawPattern := strings.TrimSpace(matches[1])
	if len(rawPattern) < 2 {
		return "", "", false
	}
	return rawPattern[1 : len(rawPattern)-1], matches[2], true
}

func augmentPatternStatsLogsQL(base, extractPattern, keepField string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		base = "*"
	}
	return fmt.Sprintf(`%s | extract "%s" | fields _time, _msg, _stream, %s | format ""`,
		base, strings.ReplaceAll(extractPattern, `"`, `\"`), keepField)
}

// ────────────────────────────────────────────────────────────────────────────
// Metric query path
// ────────────────────────────────────────────────────────────────────────────

func (d *Deps) handleMetricQuery(
	ctx *fasthttp.RequestCtx,
	result translator.Result,
	ast parser.Query,
	start, end time.Time,
	step time.Duration,
) {
	if step <= 0 {
		step = time.Minute
	}

	buckets, err := d.VL.QueryHits(reqContext(ctx), vlogs.HitsQueryRequest{
		Query: result.LogsQL,
		Start: start,
		End:   end,
		Step:  step,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeError(ctx, fasthttp.StatusGatewayTimeout, "timeout", "query timed out")
			return
		}
		slog.Error("QueryHits failed", "logsql", result.LogsQL, "err", err)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"VictoriaLogs hits query failed: "+err.Error())
		return
	}

	metric := eqLabels(ast)
	isRate := result.MetricFunc == parser.Rate
	series := loki.ShapeMatrix(buckets, metric, isRate, step.Seconds())

	writeJSON(ctx, fasthttp.StatusOK, loki.MatrixResponse{
		Status: "success",
		Data: loki.MatrixData{
			ResultType: "matrix",
			Result:     series,
		},
	})
}

// ────────────────────────────────────────────────────────────────────────────
// Aggregation query path  (sum/count/avg/min/max by (...) (count_over_time / rate))
// ────────────────────────────────────────────────────────────────────────────

// handleAggregationQuery handles vector aggregation queries such as:
//
//	sum by (detected_level) (count_over_time({app="api"}[2s]))
//
// Because VictoriaLogs has no native per-field grouped-hits API, we stream
// individual records and count them in memory by (time_bucket, group_labels).
// The time bucket is determined by floor((record_time - start) / step).
func (d *Deps) handleAggregationQuery(
	ctx *fasthttp.RequestCtx,
	result translator.Result,
	start, end time.Time,
	step time.Duration,
) {
	if step <= 0 {
		step = time.Minute
	}

	type bucketMap = map[time.Time]float64
	type sourceBucketMap = map[string]bucketMap

	seriesSamples := make(map[string]sourceBucketMap)
	seriesMetrics := make(map[string]map[string]string)
	rangeDur := end.Sub(start)
	inverseRemap := invertRemap(d.Cfg.Labels.LabelRemap)

	scanErr := d.VL.QueryLogs(reqContext(ctx), vlogs.LogQueryRequest{
		Query: result.LogsQL,
		Start: start,
		End:   end,
	}, func(rec vlogs.Record) error {
		t := parseRecordTime(rec["_time"])
		if t.IsZero() {
			return nil
		}
		offset := t.Sub(start)
		if offset < 0 || offset >= rangeDur {
			return nil // outside the requested range
		}
		bucketIdx := int64(offset / step)
		bucketTime := start.Add(time.Duration(bucketIdx) * step)
		value := 1.0
		if result.MetricFunc == parser.Rate {
			value = 1 / step.Seconds()
		}

		// Build the series key and metric from the group-by labels.
		metric := make(map[string]string, len(result.AggregateBy))
		keyParts := make([]string, 0, len(result.AggregateBy)*2)
		for _, label := range result.AggregateBy {
			val := rec[label]
			if label == "service_name" && val == "" {
				val = syntheticServiceName(rec)
			}
			outLabel := label
			if original := inverseRemap[label]; original != "" {
				outLabel = original
			}
			metric[outLabel] = val
			keyParts = append(keyParts, outLabel, val)
		}
		seriesKey := strings.Join(keyParts, "\x00")
		sourceKey := recordSeriesIdentity(rec)

		if _, exists := seriesSamples[seriesKey]; !exists {
			seriesSamples[seriesKey] = make(sourceBucketMap)
			seriesMetrics[seriesKey] = metric
		}
		if _, exists := seriesSamples[seriesKey][sourceKey]; !exists {
			seriesSamples[seriesKey][sourceKey] = make(bucketMap)
		}
		seriesSamples[seriesKey][sourceKey][bucketTime] += value
		return nil
	})

	switch {
	case scanErr == nil:
	case errors.Is(scanErr, vlogs.ErrResponseTooLarge):
		ctx.Response.Header.Set("X-Proxy-Truncated", "true")
		slog.Warn("handleAggregationQuery: response truncated", "logsql", result.LogsQL)
	case errors.Is(scanErr, context.Canceled), errors.Is(scanErr, context.DeadlineExceeded):
		writeError(ctx, fasthttp.StatusGatewayTimeout, "timeout", "query timed out")
		return
	default:
		slog.Error("handleAggregationQuery QueryLogs failed", "logsql", result.LogsQL, "err", scanErr)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"VictoriaLogs query failed: "+scanErr.Error())
		return
	}

	// Sort series keys for deterministic output.
	seriesKeys := make([]string, 0, len(seriesSamples))
	for k := range seriesSamples {
		seriesKeys = append(seriesKeys, k)
	}
	sort.Strings(seriesKeys)

	matrixResult := make([]loki.MatrixSeries, 0, len(seriesKeys))
	for _, key := range seriesKeys {
		sources := seriesSamples[key]

		timeSet := make(map[time.Time]struct{})
		for _, buckets := range sources {
			for t := range buckets {
				timeSet[t] = struct{}{}
			}
		}
		times := make([]time.Time, 0, len(timeSet))
		for t := range timeSet {
			times = append(times, t)
		}
		sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })

		values := make([][]interface{}, len(times))
		for i, t := range times {
			ts := float64(t.UnixNano()) / 1e9
			values[i] = []interface{}{ts, formatAggregationValue(aggregateBucket(result.AggregationFunc, sources, t))}
		}
		matrixResult = append(matrixResult, loki.MatrixSeries{
			Metric: seriesMetrics[key],
			Values: values,
		})
	}

	writeJSON(ctx, fasthttp.StatusOK, loki.MatrixResponse{
		Status: "success",
		Data: loki.MatrixData{
			ResultType: "matrix",
			Result:     matrixResult,
		},
	})
}

func invertRemap(remap map[string]string) map[string]string {
	if len(remap) == 0 {
		return nil
	}
	out := make(map[string]string, len(remap))
	for original, mapped := range remap {
		out[mapped] = original
	}
	return out
}

func recordSeriesIdentity(rec vlogs.Record) string {
	if stream := rec["_stream"]; stream != "" {
		return stream
	}
	labels := make(map[string]string, len(rec))
	for k, v := range rec {
		if k == "_msg" || k == "_time" {
			continue
		}
		labels[k] = v
	}
	return buildSeriesIdentity(labels)
}

func buildSeriesIdentity(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}
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
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(labels[k])
	}
	sb.WriteByte('}')
	return sb.String()
}

func aggregateBucket(fn parser.AggregationFunction, sources map[string]map[time.Time]float64, t time.Time) float64 {
	var (
		sum   float64
		min   float64
		max   float64
		count int
	)
	for _, buckets := range sources {
		v, ok := buckets[t]
		if !ok {
			continue
		}
		if count == 0 {
			min, max = v, v
		} else {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		sum += v
		count++
	}
	if count == 0 {
		return 0
	}
	switch fn {
	case parser.AggCount:
		return float64(count)
	case parser.AggAvg:
		return sum / float64(count)
	case parser.AggMin:
		return min
	case parser.AggMax:
		return max
	default:
		return sum
	}
}

func formatAggregationValue(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// parseRecordTime parses a VictoriaLogs _time field (RFC3339Nano) into a
// time.Time. Returns the zero time on any parse error.
func parseRecordTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}

// ────────────────────────────────────────────────────────────────────────────

// eqLabels extracts all equality (=) label matchers from the query's stream
// selector and returns them as a flat map. These become the "metric" labels in
// the Loki matrix response, letting Grafana display meaningful series names.
func eqLabels(q parser.Query) map[string]string {
	var matchers []parser.LabelMatcher
	switch v := q.(type) {
	case *parser.LogQuery:
		matchers = v.Selector.Matchers
	case *parser.MetricQuery:
		matchers = v.Inner.Selector.Matchers
	}
	out := make(map[string]string, len(matchers))
	for _, m := range matchers {
		if m.Type == parser.Eq {
			out[m.Name] = m.Value
		}
	}
	return out
}
