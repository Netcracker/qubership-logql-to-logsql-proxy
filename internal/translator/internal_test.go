package translator

import (
	"regexp"
	"testing"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/parser"
)

func TestTranslationErrorAndLabelFilterHelpers(t *testing.T) {
	err := (&TranslationError{Msg: "boom"}).Error()
	if err != "translation error: boom" {
		t.Errorf("Error() = %q", err)
	}

	filter, ferr := translateLabelFilter(&parser.LabelFilter{Name: "_stream", Type: parser.Neq, Value: ""}, Options{})
	if ferr != nil {
		t.Fatalf("translateLabelFilter(_stream): %v", ferr)
	}
	if filter != "" {
		t.Errorf("translateLabelFilter(_stream) = %q, want empty", filter)
	}

	filter, ferr = translateLabelFilter(&parser.LabelFilter{Name: "detected_level", Type: parser.Eq, Value: "warn"}, Options{
		LabelRemap: map[string]string{"detected_level": "level"},
	})
	if ferr != nil {
		t.Fatalf("translateLabelFilter(remap): %v", ferr)
	}
	if filter != `level:="warn"` {
		t.Errorf("translateLabelFilter(remap) = %q", filter)
	}

	if got := remapNames([]string{"detected_level", "app"}, map[string]string{"detected_level": "level"}); len(got) != 2 || got[0] != "level" || got[1] != "app" {
		t.Errorf("remapNames() = %v", got)
	}

	stream, streamErr := translateMatcher(parser.LabelMatcher{
		Name:  "_stream",
		Type:  parser.Eq,
		Value: `{container="cloud-provider-kind",namespace="kube-system"}`,
	}, Options{})
	if streamErr != nil {
		t.Fatalf("translateMatcher(_stream): %v", streamErr)
	}
	if stream != `{container="cloud-provider-kind",namespace="kube-system"}` {
		t.Errorf("translateMatcher(_stream) = %q", stream)
	}

	timeFilter, timeErr := translateMatcher(parser.LabelMatcher{
		Name:  "_time",
		Type:  parser.Neq,
		Value: "",
	}, Options{})
	if timeErr != nil {
		t.Fatalf("translateMatcher(_time): %v", timeErr)
	}
	if timeFilter != "" {
		t.Errorf("translateMatcher(_time) = %q, want empty", timeFilter)
	}

	if got := escapeRe(`\"(?:[0-9]{1,3}\.){3}[0-9]{1,3}\"`); got != `\\\"(?:[0-9]{1,3}\\.){3}[0-9]{1,3}\\\"` {
		t.Errorf("escapeRe() = %q", got)
	}
}

func TestLogQLPatternToRegexMatchesVerboseVmalertURLPattern(t *testing.T) {
	pattern := `<DATETIME>	error	VictoriaMetrics/app/vmalert/rule/group.go:<N>	group "Etcd": rule "EtcdHighNumberOfFailedGrpcRequests": failed to execute: failed to execute query "sum(rate(grpc_server_handled_total{job=\"etcd\",grpc_code!=\"OK\", grpc_method!=\"Watch\"}[<N>m])) BY (grpc_service, grpc_method) / sum(rate(grpc_server_handled_total{job=\"etcd\"}[<N>m])) BY (grpc_service, grpc_method) > <N>.<N>": error getting response from https://vmsingle-k8s.monitoring.svc:<N>/api/v<N>/query?query=sum%28rate%28grpc_server_handled_total%7Bjob%3D%22etcd%22%2Cgrpc_code%21%3D%22OK%22%2C+grpc_method%21%3D%22Watch%22%7D%5B5m%5D%29%29+BY+%28grpc_service%2C+grpc_method%29+%2F+sum%28rate%28grpc_server_handled_total%7Bjob%3D%22etcd%22%7D%5B5m%5D%29%29+BY+%28grpc_service%2C+grpc_method%29+%3E+<N>.<N>&step=<N>s&time=<DATE>T<N>%<N>%<N>Z: Post "https://vmsingle-k8s.monitoring.svc:<N>/api/v<N>/query?query=sum%28rate%28grpc_server_handled_total%7Bjob%3D%22etcd%22%2Cgrpc_code%21%3D%22OK%22%2C%20grpc_method%21%3D%22Watch%22%7D%5B5m%5D%29%29%20BY%20%28grpc_service%2C%20grpc_method%29%20%2F%20sum%28rate%28grpc_server_handled_total%7Bjob%3D%22etcd%22%7D%5B5m%5D%29%29%20BY%20%28grpc_service%2C%20grpc_method%29%20%3E%200.1&step=<N>s&time=<DATE>T<N>%<N>%<N>Z": tls: failed to verify certificate: x<N>: certificate signed by unknown authority`
	raw := `2026-05-13T06:52:22Z	error	VictoriaMetrics/app/vmalert/rule/group.go:123	group "Etcd": rule "EtcdHighNumberOfFailedGrpcRequests": failed to execute: failed to execute query "sum(rate(grpc_server_handled_total{job=\"etcd\",grpc_code!=\"OK\", grpc_method!=\"Watch\"}[5m])) BY (grpc_service, grpc_method) / sum(rate(grpc_server_handled_total{job=\"etcd\"}[5m])) BY (grpc_service, grpc_method) > 0.1": error getting response from https://vmsingle-k8s.monitoring.svc:8428/api/v1/query?query=sum%28rate%28grpc_server_handled_total%7Bjob%3D%22etcd%22%2Cgrpc_code%21%3D%22OK%22%2C%20grpc_method%21%3D%22Watch%22%7D%5B5m%5D%29%29%20BY%20%28grpc_service%2C%20grpc_method%29%20%2F%20sum%28rate%28grpc_server_handled_total%7Bjob%3D%22etcd%22%7D%5B5m%5D%29%29%20BY%20%28grpc_service%2C%20grpc_method%29%20%3E%200.1&step=15s&time=2026-05-13T06%3A52%3A22Z: Post "https://vmsingle-k8s.monitoring.svc:8428/api/v1/query?query=sum%28rate%28grpc_server_handled_total%7Bjob%3D%22etcd%22%2Cgrpc_code%21%3D%22OK%22%2C%20grpc_method%21%3D%22Watch%22%7D%5B5m%5D%29%29%20BY%20%28grpc_service%2C%20grpc_method%29%20%2F%20sum%28rate%28grpc_server_handled_total%7Bjob%3D%22etcd%22%7D%5B5m%5D%29%29%20BY%20%28grpc_service%2C%20grpc_method%29%20%3E%200.1&step=15s&time=2026-05-13T06%3A52%3A22Z": tls: failed to verify certificate: x509: certificate signed by unknown authority`

	re := logQLPatternToRegex(pattern)
	matched, err := regexp.MatchString(re, raw)
	if err != nil {
		t.Fatalf("regexp.MatchString(): %v\nregex=%s", err, re)
	}
	if !matched {
		t.Fatalf("regex did not match raw log\nregex=%s\nraw=%s", re, raw)
	}
}

func TestLogQLPatternToRegexMatchesKubernetesRevisionSuffixPattern(t *testing.T) {
	pattern := `updating pod=vmalertmanager-k8s-<N> revision label="vmalertmanager-k8s-<N>"`
	raw := `updating pod=vmalertmanager-k8s-8646d74c8d revision label="vmalertmanager-k8s-8646d74c8d"`

	re := logQLPatternToRegex(pattern)
	matched, err := regexp.MatchString(re, raw)
	if err != nil {
		t.Fatalf("regexp.MatchString(): %v\nregex=%s", err, re)
	}
	if !matched {
		t.Fatalf("regex did not match raw log\nregex=%s\nraw=%s", re, raw)
	}
}
