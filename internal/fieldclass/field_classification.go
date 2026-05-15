package fieldclass

import (
	"cmp"
	"slices"
	"strings"
	"unicode"

	"github.com/netcracker/qubership-logql-to-logsql-proxy/internal/config"
)

// FieldClass describes how the proxy should treat a VictoriaLogs field when
// surfacing it to Grafana-facing metadata and log-detail flows.
type FieldClass string

const (
	FieldClassLabel              FieldClass = "label"
	FieldClassParsed             FieldClass = "parsed"
	FieldClassStructuredMetadata FieldClass = "structured_metadata"
	FieldClassExcluded           FieldClass = "excluded"
	FieldClassUnclassified       FieldClass = "unclassified"
)

func Classify(name string, cfg config.LabelsConfig) FieldClass {
	if IsExcluded(name, cfg) {
		return FieldClassExcluded
	}
	if IsKnownLabel(name, cfg) {
		return FieldClassLabel
	}
	if slices.Contains(cfg.KnownParsedFields, name) {
		return FieldClassParsed
	}
	if slices.Contains(cfg.KnownStructuredMetadata, name) {
		return FieldClassStructuredMetadata
	}
	return FieldClassUnclassified
}

func IsKnownLabel(name string, cfg config.LabelsConfig) bool {
	if slices.Contains(cfg.KnownLabels, name) {
		return true
	}
	for original, mapped := range cfg.LabelRemap {
		if mapped == name && slices.Contains(cfg.KnownLabels, original) {
			return true
		}
	}
	return false
}

// DisplayLabelName returns the user-facing label name that should be exposed to
// Grafana for a backend field. For remapped labels such as detected_level ->
// level, Grafana should continue seeing the original LogQL-facing label name.
func DisplayLabelName(name string, cfg config.LabelsConfig) string {
	if slices.Contains(cfg.KnownLabels, name) {
		return name
	}
	for _, original := range cfg.KnownLabels {
		if cfg.LabelRemap[original] == name {
			return original
		}
	}
	return name
}

// NormalizeDetectedLevel maps raw backend level values to Grafana Logs
// Drilldown-compatible detected_level values such as info, warn and error.
// It accepts both canonical words and single-letter shorthands commonly found
// in klog-style records.
func NormalizeDetectedLevel(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return ""
	}
	switch strings.ToLower(v) {
	case "t", "trace":
		return "trace"
	case "d", "debug":
		return "debug"
	case "i", "info", "information":
		return "info"
	case "w", "warn", "warning":
		return "warn"
	case "e", "err", "error":
		return "error"
	case "c", "critical":
		return "critical"
	case "f", "fatal":
		return "fatal"
	default:
		return strings.ToLower(v)
	}
}

func IsExcluded(name string, cfg config.LabelsConfig) bool {
	return slices.Contains(cfg.ExcludedFields, name)
}

func ShouldExposeDetectedField(name string, cfg config.LabelsConfig) bool {
	switch Classify(name, cfg) {
	case FieldClassLabel, FieldClassExcluded:
		return false
	}
	return !IsSuppressedDetectedFieldName(name)
}

func IsSuppressedDetectedFieldName(name string) bool {
	if name == "" {
		return true
	}
	switch name {
	case "__error__", "__error_details__", "timestamp_end", "observed_timestamp_end":
		return true
	}
	if strings.Contains(name, "{") || strings.Contains(name, "}") ||
		strings.Contains(name, "\"") || strings.Contains(name, "'") ||
		strings.Contains(name, "(") || strings.Contains(name, ")") ||
		strings.Contains(name, ",") || strings.Contains(name, "%") {
		return true
	}
	if strings.HasSuffix(name, ":") {
		return true
	}
	for _, r := range name {
		if unicode.IsSpace(r) {
			return true
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) &&
			r != '_' && r != '.' && r != '/' && r != '-' {
			return true
		}
	}
	return false
}

func SortDetectedFieldNames(names []string, cfg config.LabelsConfig) {
	slices.SortStableFunc(names, func(a, b string) int {
		ra := detectedFieldRank(a, cfg)
		rb := detectedFieldRank(b, cfg)
		if ra != rb {
			return cmp.Compare(ra, rb)
		}
		return cmp.Compare(a, b)
	})
}

func detectedFieldRank(name string, cfg config.LabelsConfig) int {
	switch Classify(name, cfg) {
	case FieldClassParsed:
		return 0
	case FieldClassStructuredMetadata:
		return 1
	case FieldClassUnclassified:
		return 2
	default:
		return 3
	}
}
