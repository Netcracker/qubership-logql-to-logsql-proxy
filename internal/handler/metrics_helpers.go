package handler

import (
	"errors"
	"strings"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/metrics"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/translator"
)

func normalizedRoute(path string) string {
	switch {
	case strings.HasPrefix(path, "/loki/api/v1/label/") && strings.HasSuffix(path, "/values"):
		return "/loki/api/v1/label/:name/values"
	case strings.HasPrefix(path, "/loki/api/v1/detected_field/") && strings.HasSuffix(path, "/values"):
		return "/loki/api/v1/detected_field/:name/values"
	default:
		return path
	}
}

func parseLogQLWithMetrics(ctx *fasthttp.RequestCtx, queryStr string) (parser.Query, bool) {
	start := time.Now()
	ast, err := parser.Parse(queryStr)
	metrics.ObserveParseDuration(time.Since(start))
	if err == nil {
		return ast, true
	}

	var unsupportedErr *parser.UnsupportedError
	if errors.As(err, &unsupportedErr) {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data",
			"unsupported LogQL construct: "+unsupportedErr.Construct)
		return nil, false
	}

	writeError(ctx, fasthttp.StatusBadRequest, "bad_data",
		"invalid LogQL query: "+err.Error())
	return nil, false
}

func translateQueryWithMetrics(ctx *fasthttp.RequestCtx, ast parser.Query, opts translator.Options) (translator.Result, bool) {
	start := time.Now()
	result, err := translator.Translate(ast, opts)
	metrics.ObserveTranslateDuration(time.Since(start))
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "bad_data", err.Error())
		return translator.Result{}, false
	}
	return result, true
}
