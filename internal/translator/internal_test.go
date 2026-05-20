package translator

import (
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

	if got := remapNames([]string{"detected_level", "app"}, map[string]string{"detected_level": "level"}); len(got) != 2 || got[0] != "level" || got[1] != "app" {
		t.Errorf("remapNames() = %v", got)
	}
}
