package handler

import (
	"strings"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/metrics"
)

// BuildHandler constructs the full fasthttp.RequestHandler with all Loki-compatible
// routes registered. It is shared between main.go and handler_test.go to
// guarantee that the test routing mirrors production.
//
// fasthttp/router (v1.5.4) has a radix-tree bug where parametric routes such
// as /loki/api/v1/label/:name/values become unreachable when static sibling
// routes like /loki/api/v1/labels are also registered. To work around this,
// "* /values" routes are matched manually in the returned wrapper handler.
func (d *Deps) BuildHandler() fasthttp.RequestHandler {
	r := router.New()

	// Loki-compatible query endpoints.
	r.GET("/loki/api/v1/query_range", d.QueryRange)
	r.GET("/loki/api/v1/query", d.Query)

	// Loki label discovery endpoints.
	r.GET("/loki/api/v1/labels", d.Labels)
	// NOTE: /loki/api/v1/label/:name/values is handled below via manual extraction.
	r.GET("/loki/api/v1/series", d.Series)
	r.GET("/loki/api/v1/detected_labels", d.DetectedLabels)
	r.GET("/loki/api/v1/detected_fields", d.DetectedFields)

	// Stub / health endpoints.
	r.GET("/loki/api/v1/index/stats", d.IndexStats)
	r.GET("/loki/api/v1/index/volume", d.IndexVolume)
	r.GET("/loki/api/v1/index/volume_range", d.IndexVolume)
	r.GET("/loki/api/v1/drilldown-limits", d.DrilldownLimits)
	r.GET("/loki/api/v1/patterns", d.Patterns)
	r.GET("/metrics", metrics.Handler())
	r.GET("/ready", Ready)

	inner := r.Handler
	labelValuesHandler := d.LabelValues
	detectedFieldValuesHandler := d.DetectedFieldValues

	return func(ctx *fasthttp.RequestCtx) {
		// Manual matches for "* /values" routes to work around the
		// fasthttp/router radix-tree bug with static sibling routes.
		if string(ctx.Method()) == "GET" {
			if name := extractSingleSegmentName(string(ctx.Path()), "/loki/api/v1/label/", "/values"); name != "" {
				ctx.SetUserValue("name", name)
				labelValuesHandler(ctx)
				return
			}
			if name := extractSingleSegmentName(string(ctx.Path()), "/loki/api/v1/detected_field/", "/values"); name != "" {
				ctx.SetUserValue("name", name)
				detectedFieldValuesHandler(ctx)
				return
			}
		}
		inner(ctx)
	}
}

// extractSingleSegmentName returns the name segment from a path matching
// {prefix}{name}{suffix}, or "" if the path does not match.
func extractSingleSegmentName(path, prefix, suffix string) string {
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return ""
	}
	name := path[len(prefix) : len(path)-len(suffix)]
	// Reject empty names or names containing slashes (not a single segment).
	if name == "" || strings.ContainsRune(name, '/') {
		return ""
	}
	return name
}
