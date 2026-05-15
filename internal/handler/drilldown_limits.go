package handler

import "github.com/valyala/fasthttp"

// DrilldownLimits handles GET /loki/api/v1/drilldown-limits.
// Grafana Logs Drilldown queries this endpoint for per-tenant UI configuration
// (max query length, ingestion rates, etc.). We return a static response that
// mirrors a minimal Loki configuration so Logs Drilldown behaves correctly
// without requiring a real Loki backend.
func (d *Deps) DrilldownLimits(ctx *fasthttp.RequestCtx) {
	cfg := d.Cfg.Drilldown
	writeJSON(ctx, fasthttp.StatusOK, map[string]interface{}{
		"limits": map[string]interface{}{
			"discover_log_levels":         cfg.DiscoverLogLevels,
			"discover_service_name":       cfg.DiscoverServiceName,
			"log_level_fields":            cfg.LogLevelFields,
			"max_entries_limit_per_query": cfg.MaxEntriesLimitPerQuery,
			"max_line_size_truncate":      cfg.MaxLineSizeTruncate,
			"max_query_bytes_read":        cfg.MaxQueryBytesRead,
			"max_query_length":            cfg.MaxQueryLength,
			"max_query_lookback":          cfg.MaxQueryLookback,
			"max_query_range":             cfg.MaxQueryRange,
			"max_query_series":            cfg.MaxQuerySeries,
			"metric_aggregation_enabled":  cfg.MetricAggregationEnabled,
			"otlp_config": map[string]interface{}{
				"resource_attributes": map[string]interface{}{
					"attributes_config": []map[string]interface{}{
						{
							"action":     "index_label",
							"attributes": cfg.OTLPIndexLabelAttributes,
						},
					},
				},
			},
			"pattern_persistence_enabled": cfg.PatternPersistenceEnabled,
			"query_timeout":               cfg.QueryTimeout,
			"retention_period":            cfg.RetentionPeriod,
			"volume_enabled":              cfg.VolumeEnabled,
			"volume_max_series":           cfg.VolumeMaxSeries,
		},
		"pattern_ingester_enabled": cfg.PatternIngesterEnabled,
		"version":                  cfg.Version,
	})
}
