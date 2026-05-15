package handler

import (
	"log/slog"
	"sort"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/fieldclass"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/loki"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/vlogs"
)

// LabelValues handles GET /loki/api/v1/label/:name/values.
//
// Returns the distinct values for the requested label name, used by Grafana to
// populate label-value filter dropdowns. Results are cached per field+time-range
// bucket using the same TTL as label names.
func (d *Deps) LabelValues(ctx *fasthttp.RequestCtx) {
	name, _ := ctx.UserValue("name").(string)
	if name == "" {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", "label name is required")
		return
	}

	start, end, err := parseTimeRange(ctx)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	// Cache lookup.
	key := vlogs.FieldValuesKey(name, start, end)
	if cached, ok := d.Cache.Get(key); ok {
		writeJSON(ctx, fasthttp.StatusOK, loki.LabelValuesResponse{Status: "success", Data: cached})
		return
	}

	values, err := d.labelValuesForName(ctx, name, start, end)
	if err != nil {
		slog.Error("FieldValues failed", "label", name, "err", err)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"failed to retrieve label values from VictoriaLogs")
		return
	}

	if d.Cfg.Labels.MetadataCacheTTL > 0 {
		d.Cache.Set(key, values, d.Cfg.Labels.MetadataCacheTTL)
	}

	writeJSON(ctx, fasthttp.StatusOK, loki.LabelValuesResponse{Status: "success", Data: values})
}

// DetectedFieldValues handles GET /loki/api/v1/detected_field/:name/values.
//
// Grafana Logs Drilldown calls this endpoint for field-specific filter options
// such as the log level picker. Unlike /label/:name/values, this variant also
// carries a LogQL query that scopes the value discovery to the currently
// selected stream set, so we apply bestEffortLogsQLFilter to preserve that
// context. The synthetic detected_level field is remapped via LabelRemap.
func (d *Deps) DetectedFieldValues(ctx *fasthttp.RequestCtx) {
	name, _ := ctx.UserValue("name").(string)
	if name == "" {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", "field name is required")
		return
	}
	if _, syntheticField := d.Cfg.Labels.LabelRemap[name]; !syntheticField {
		if class := fieldclass.Classify(name, d.Cfg.Labels); class == fieldclass.FieldClassExcluded || class == fieldclass.FieldClassLabel {
			writeJSON(ctx, fasthttp.StatusOK, loki.LabelValuesResponse{Status: "success", Data: []string{}})
			return
		}
	}

	start, end, err := parseTimeRange(ctx)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	fieldName := name
	if mapped, ok := d.Cfg.Labels.LabelRemap[name]; ok {
		fieldName = mapped
	}
	queryStr := string(ctx.QueryArgs().Peek("query"))
	logsqlFilter := bestEffortLogsQLFilter(queryStr, translator.Options{LabelRemap: d.Cfg.Labels.LabelRemap})

	values, err := d.VL.FieldValues(reqContext(ctx), vlogs.FieldValuesRequest{
		FieldName: fieldName,
		Query:     logsqlFilter,
		Start:     start,
		End:       end,
		Limit:     d.Cfg.Limits.MaxLimit,
	})
	if err != nil {
		slog.Error("FieldValues failed (detected_field_values)", "field", name, "mappedField", fieldName, "err", err)
		writeError(ctx, fasthttp.StatusBadGateway, "execution",
			"failed to retrieve field values from VictoriaLogs")
		return
	}
	if name == "detected_level" {
		values = normalizeDetectedLevelValues(values)
	}

	writeJSON(ctx, fasthttp.StatusOK, loki.LabelValuesResponse{Status: "success", Data: values})
}

func (d *Deps) labelValuesForName(ctx *fasthttp.RequestCtx, name string, start, end time.Time) ([]string, error) {
	if name == "service_name" {
		return d.syntheticServiceNameValues(ctx, start, end)
	}

	fieldName := name
	if mapped, ok := d.Cfg.Labels.LabelRemap[name]; ok && mapped != "" {
		fieldName = mapped
	}

	return d.VL.FieldValues(reqContext(ctx), vlogs.FieldValuesRequest{
		FieldName: fieldName,
		Query:     "*",
		Start:     start,
		End:       end,
		Limit:     d.Cfg.Limits.MaxLimit,
	})
}

func (d *Deps) syntheticServiceNameValues(ctx *fasthttp.RequestCtx, start, end time.Time) ([]string, error) {
	seen := make(map[string]struct{})
	values := make([]string, 0)

	err := d.VL.QueryLogs(reqContext(ctx), vlogs.LogQueryRequest{
		Query: "*",
		Start: start,
		End:   end,
	}, func(rec vlogs.Record) error {
		val := syntheticServiceName(rec)
		if val == "" {
			return nil
		}
		if _, ok := seen[val]; ok {
			return nil
		}
		seen[val] = struct{}{}
		values = append(values, val)
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(values)
	return values, nil
}

func normalizeDetectedLevelValues(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := fieldclass.NormalizeDetectedLevel(value)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}
