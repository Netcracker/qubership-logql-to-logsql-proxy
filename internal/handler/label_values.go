package handler

import (
	"log/slog"

	"github.com/valyala/fasthttp"

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
	if !nameAllowed(name, d.Cfg.Labels.AllowLabels, d.Cfg.Labels.DenyLabels) {
		writeJSON(ctx, fasthttp.StatusOK, loki.LabelValuesResponse{Status: "success", Data: []string{}})
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

	values, err := d.VL.FieldValues(reqContext(ctx), vlogs.FieldValuesRequest{
		FieldName: name,
		Query:     "*",
		Start:     start,
		End:       end,
		Limit:     d.Cfg.Limits.MaxLimit,
	})
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
// Grafana Logs Drilldown calls this endpoint for field-scoped filter dropdowns
// such as the Log levels picker. Unlike /label/:name/values, this endpoint
// includes the current LogQL query, so we preserve that context via
// bestEffortLogsQLFilter. Synthetic field names such as detected_level are
// remapped through LabelRemap before querying VictoriaLogs.
func (d *Deps) DetectedFieldValues(ctx *fasthttp.RequestCtx) {
	name, _ := ctx.UserValue("name").(string)
	if name == "" {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", "field name is required")
		return
	}
	if !nameAllowed(name, d.Cfg.Labels.AllowFields, d.Cfg.Labels.DenyFields) {
		writeJSON(ctx, fasthttp.StatusOK, loki.LabelValuesResponse{Status: "success", Data: []string{}})
		return
	}

	start, end, err := parseTimeRange(ctx)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return
	}

	fieldName := name
	if mapped, ok := d.Cfg.Labels.LabelRemap[name]; ok && mapped != "" {
		fieldName = mapped
	}
	queryStr := string(ctx.QueryArgs().Peek("query"))
	logsqlFilter := bestEffortLogsQLFilter(queryStr, translator.Options{
		LabelRemap:                d.Cfg.Labels.LabelRemap,
		ServiceNameFallbackFields: d.Cfg.Labels.ServiceNameFallbackFields,
	})

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

	writeJSON(ctx, fasthttp.StatusOK, loki.LabelValuesResponse{Status: "success", Data: values})
}

func nameAllowed(name string, allow, deny []string) bool {
	if len(allow) > 0 {
		allowed := false
		for _, item := range allow {
			if item == name {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}
	for _, item := range deny {
		if item == name {
			return false
		}
	}
	return true
}
