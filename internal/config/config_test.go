package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadAppliesDefaultsFileEnvAndPasswordFile(t *testing.T) {
	passFile, err := os.CreateTemp(t.TempDir(), "password")
	if err != nil {
		t.Fatalf("CreateTemp(password): %v", err)
	}
	if _, err := passFile.WriteString("super-secret\n"); err != nil {
		t.Fatalf("WriteString(password): %v", err)
	}
	if err := passFile.Close(); err != nil {
		t.Fatalf("Close(password): %v", err)
	}

	cfgFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp(config): %v", err)
	}
	cfgYAML := strings.Join([]string{
		"vlogs:",
		"  url: http://victorialogs:9428",
		"  basicAuth:",
		"    username: demo",
		"    passwordFile: " + passFile.Name(),
		"labels:",
		"  metadataCacheTTL: 30s",
		"log:",
		"  format: text",
		"",
	}, "\n")
	if _, err := cfgFile.WriteString(cfgYAML); err != nil {
		t.Fatalf("WriteString(config): %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("Close(config): %v", err)
	}

	t.Setenv("PROXY_SERVER_LISTENADDR", ":9999")
	t.Setenv("PROXY_LABELS_KNOWNLABELS", " app , team,, env ")
	t.Setenv("PROXY_LABELS_ALLOWLABELS", " app , env ")
	t.Setenv("PROXY_LABELS_DENYFIELDS", " _stream , _stream_id ")
	t.Setenv("PROXY_LOG_LEVEL", "debug")

	cfg, err := Load(cfgFile.Name())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if cfg.Server.ListenAddr != ":9999" {
		t.Errorf("ListenAddr = %q, want %q", cfg.Server.ListenAddr, ":9999")
	}
	if cfg.Server.ReadTimeout != 30*time.Second {
		t.Errorf("ReadTimeout = %v, want %v", cfg.Server.ReadTimeout, 30*time.Second)
	}
	if cfg.Server.ReadBufferSize != 64*1024 {
		t.Errorf("ReadBufferSize = %d, want %d", cfg.Server.ReadBufferSize, 64*1024)
	}
	if cfg.VLogs.URL != "http://victorialogs:9428" {
		t.Errorf("VLogs.URL = %q, want %q", cfg.VLogs.URL, "http://victorialogs:9428")
	}
	if cfg.VLogs.BasicAuth == nil {
		t.Fatal("expected BasicAuth to be populated")
	}
	if cfg.VLogs.BasicAuth.Username != "demo" {
		t.Errorf("BasicAuth.Username = %q, want %q", cfg.VLogs.BasicAuth.Username, "demo")
	}
	if cfg.VLogs.BasicAuth.Password != "super-secret" {
		t.Errorf("BasicAuth.Password = %q, want trimmed file contents", cfg.VLogs.BasicAuth.Password)
	}
	if cfg.Labels.MetadataCacheTTL != 30*time.Second {
		t.Errorf("MetadataCacheTTL = %v, want %v", cfg.Labels.MetadataCacheTTL, 30*time.Second)
	}
	if got := cfg.Labels.KnownLabels; len(got) != 3 || got[0] != "app" || got[1] != "team" || got[2] != "env" {
		t.Errorf("KnownLabels = %v, want [app team env]", got)
	}
	if got := cfg.Labels.AllowLabels; len(got) != 2 || got[0] != "app" || got[1] != "env" {
		t.Errorf("AllowLabels = %v, want [app env]", got)
	}
	if got := cfg.Labels.DenyFields; len(got) != 2 || got[0] != "_stream" || got[1] != "_stream_id" {
		t.Errorf("DenyFields = %v, want [_stream _stream_id]", got)
	}
	if len(cfg.Labels.ServiceNameFallbackFields) == 0 {
		t.Errorf("ServiceNameFallbackFields should be populated by default")
	}
	if len(cfg.Labels.LabelRemap) != 0 {
		t.Errorf("LabelRemap = %v, want empty by default", cfg.Labels.LabelRemap)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q, want %q", cfg.Log.Level, "debug")
	}
	if cfg.Log.Format != "text" {
		t.Errorf("Log.Format = %q, want %q", cfg.Log.Format, "text")
	}
	if cfg.Limits.MaxStreamsPerResponse != 5000 {
		t.Errorf("MaxStreamsPerResponse = %d, want default 5000", cfg.Limits.MaxStreamsPerResponse)
	}
	if cfg.Limits.AggregationScanLimit != 0 {
		t.Errorf("AggregationScanLimit = %d, want default 0", cfg.Limits.AggregationScanLimit)
	}
}

func TestLoadAllowsExplicitLabelRemapFromConfig(t *testing.T) {
	cfgFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp(config): %v", err)
	}
	cfgYAML := strings.Join([]string{
		"vlogs:",
		"  url: http://victorialogs:9428",
		"labels:",
		"  labelRemap:",
		"    detected_level: level",
		"",
	}, "\n")
	if _, err := cfgFile.WriteString(cfgYAML); err != nil {
		t.Fatalf("WriteString(config): %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("Close(config): %v", err)
	}

	cfg, err := Load(cfgFile.Name())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if got := cfg.Labels.LabelRemap["detected_level"]; got != "level" {
		t.Fatalf("LabelRemap[detected_level] = %q, want %q", got, "level")
	}
}

func TestLoadAllowsExplicitServiceNameFallbackFieldsFromConfig(t *testing.T) {
	cfgFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp(config): %v", err)
	}
	cfgYAML := strings.Join([]string{
		"vlogs:",
		"  url: http://victorialogs:9428",
		"labels:",
		"  serviceNameFallbackFields:",
		"    - svc",
		"    - app",
		"",
	}, "\n")
	if _, err := cfgFile.WriteString(cfgYAML); err != nil {
		t.Fatalf("WriteString(config): %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("Close(config): %v", err)
	}

	cfg, err := Load(cfgFile.Name())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if got := cfg.Labels.ServiceNameFallbackFields; len(got) != 2 || got[0] != "svc" || got[1] != "app" {
		t.Fatalf("ServiceNameFallbackFields = %v, want [svc app]", got)
	}
}

func TestLoadAllowsExplicitAggregationScanLimitFromConfig(t *testing.T) {
	cfgFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp(config): %v", err)
	}
	cfgYAML := strings.Join([]string{
		"vlogs:",
		"  url: http://victorialogs:9428",
		"limits:",
		"  aggregationScanLimit: 1234",
		"",
	}, "\n")
	if _, err := cfgFile.WriteString(cfgYAML); err != nil {
		t.Fatalf("WriteString(config): %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("Close(config): %v", err)
	}

	cfg, err := Load(cfgFile.Name())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if cfg.Limits.AggregationScanLimit != 1234 {
		t.Fatalf("AggregationScanLimit = %d, want 1234", cfg.Limits.AggregationScanLimit)
	}
}

func TestLoadAllowsExplicitDrilldownLimitsFromConfig(t *testing.T) {
	cfgFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp(config): %v", err)
	}
	cfgYAML := strings.Join([]string{
		"vlogs:",
		"  url: http://victorialogs:9428",
		"drilldownLimits:",
		"  discoverServiceName:",
		"    - service_name",
		"    - app",
		"  logLevelFields:",
		"    - detected_level",
		"  maxEntriesLimitPerQuery: 2048",
		"  maxQuerySeries: 900",
		"  queryTimeout: 45s",
		"  volumeMaxSeries: 123456",
		"  version: custom",
		"",
	}, "\n")
	if _, err := cfgFile.WriteString(cfgYAML); err != nil {
		t.Fatalf("WriteString(config): %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("Close(config): %v", err)
	}

	cfg, err := Load(cfgFile.Name())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if got := cfg.DrilldownLimits.DiscoverServiceName; len(got) != 2 || got[0] != "service_name" || got[1] != "app" {
		t.Fatalf("DiscoverServiceName = %v, want [service_name app]", got)
	}
	if got := cfg.DrilldownLimits.LogLevelFields; len(got) != 1 || got[0] != "detected_level" {
		t.Fatalf("LogLevelFields = %v, want [detected_level]", got)
	}
	if cfg.DrilldownLimits.MaxEntriesLimitPerQuery != 2048 {
		t.Fatalf("MaxEntriesLimitPerQuery = %d, want 2048", cfg.DrilldownLimits.MaxEntriesLimitPerQuery)
	}
	if cfg.DrilldownLimits.MaxQuerySeries != 900 {
		t.Fatalf("MaxQuerySeries = %d, want 900", cfg.DrilldownLimits.MaxQuerySeries)
	}
	if cfg.DrilldownLimits.QueryTimeout != "45s" {
		t.Fatalf("QueryTimeout = %q, want 45s", cfg.DrilldownLimits.QueryTimeout)
	}
	if cfg.DrilldownLimits.VolumeMaxSeries != 123456 {
		t.Fatalf("VolumeMaxSeries = %d, want 123456", cfg.DrilldownLimits.VolumeMaxSeries)
	}
	if cfg.DrilldownLimits.Version != "custom" {
		t.Fatalf("Version = %q, want custom", cfg.DrilldownLimits.Version)
	}
}

func TestLoadDefaultsDrilldownLimitsFromRuntimeLimits(t *testing.T) {
	cfgFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp(config): %v", err)
	}
	cfgYAML := strings.Join([]string{
		"vlogs:",
		"  url: http://victorialogs:9428",
		"  timeout: 45s",
		"limits:",
		"  maxLimit: 2048",
		"  maxStreamsPerResponse: 321",
		"",
	}, "\n")
	if _, err := cfgFile.WriteString(cfgYAML); err != nil {
		t.Fatalf("WriteString(config): %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("Close(config): %v", err)
	}

	cfg, err := Load(cfgFile.Name())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if cfg.DrilldownLimits.MaxEntriesLimitPerQuery != 2048 {
		t.Fatalf("MaxEntriesLimitPerQuery = %d, want 2048", cfg.DrilldownLimits.MaxEntriesLimitPerQuery)
	}
	if cfg.DrilldownLimits.MaxQuerySeries != 321 {
		t.Fatalf("MaxQuerySeries = %d, want 321", cfg.DrilldownLimits.MaxQuerySeries)
	}
	if cfg.DrilldownLimits.QueryTimeout != "45s" {
		t.Fatalf("QueryTimeout = %q, want 45s", cfg.DrilldownLimits.QueryTimeout)
	}
}

func TestLoadAllowsExplicitLabelAndFieldFiltersFromConfig(t *testing.T) {
	cfgFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp(config): %v", err)
	}
	cfgYAML := strings.Join([]string{
		"vlogs:",
		"  url: http://victorialogs:9428",
		"labels:",
		"  allowLabels:",
		"    - app",
		"    - namespace",
		"  denyLabels:",
		"    - _stream",
		"  allowFields:",
		"    - level",
		"  denyFields:",
		"    - _msg",
		"",
	}, "\n")
	if _, err := cfgFile.WriteString(cfgYAML); err != nil {
		t.Fatalf("WriteString(config): %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("Close(config): %v", err)
	}

	cfg, err := Load(cfgFile.Name())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if got := cfg.Labels.AllowLabels; len(got) != 2 || got[0] != "app" || got[1] != "namespace" {
		t.Fatalf("AllowLabels = %v, want [app namespace]", got)
	}
	if got := cfg.Labels.DenyLabels; len(got) != 1 || got[0] != "_stream" {
		t.Fatalf("DenyLabels = %v, want [_stream]", got)
	}
	if got := cfg.Labels.AllowFields; len(got) != 1 || got[0] != "level" {
		t.Fatalf("AllowFields = %v, want [level]", got)
	}
	if got := cfg.Labels.DenyFields; len(got) != 1 || got[0] != "_msg" {
		t.Fatalf("DenyFields = %v, want [_msg]", got)
	}
}

func TestLoadReturnsConversionErrorForInvalidDuration(t *testing.T) {
	cfgFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp(config): %v", err)
	}
	if _, err := cfgFile.WriteString("vlogs:\n  url: http://victorialogs:9428\nserver:\n  readTimeout: nope\n"); err != nil {
		t.Fatalf("WriteString(config): %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("Close(config): %v", err)
	}

	_, err = Load(cfgFile.Name())
	if err == nil {
		t.Fatal("expected invalid duration error, got nil")
	}
	if !strings.Contains(err.Error(), `server.readTimeout: invalid duration "nope"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadAllowsExplicitReadBufferSizeFromConfig(t *testing.T) {
	cfgFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp(config): %v", err)
	}
	cfgYAML := strings.Join([]string{
		"vlogs:",
		"  url: http://victorialogs:9428",
		"server:",
		"  readBufferSize: 131072",
		"",
	}, "\n")
	if _, err := cfgFile.WriteString(cfgYAML); err != nil {
		t.Fatalf("WriteString(config): %v", err)
	}
	if err := cfgFile.Close(); err != nil {
		t.Fatalf("Close(config): %v", err)
	}

	cfg, err := Load(cfgFile.Name())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if cfg.Server.ReadBufferSize != 131072 {
		t.Fatalf("ReadBufferSize = %d, want 131072", cfg.Server.ReadBufferSize)
	}
}

func TestValidateReturnsCombinedErrors(t *testing.T) {
	cfg := &Config{
		Limits: LimitsConfig{
			MaxConcurrentQueries:  0,
			MaxQueueDepth:         -1,
			MaxResponseBodyBytes:  0,
			MaxStreamsPerResponse: 0,
			AggregationScanLimit:  -1,
			MaxMemoryMB:           0,
			MaxQueryRangeHours:    0,
			MaxLimit:              1,
			DefaultLimit:          2,
		},
		Log: LogConfig{
			Level:  "verbose",
			Format: "xml",
		},
	}

	err := validate(cfg)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}

	errStr := err.Error()
	for _, want := range []string{
		"vlogs.url is required",
		"limits.maxConcurrentQueries must be >= 1",
		"limits.maxQueueDepth must be >= 0",
		"limits.maxResponseBodyBytes must be >= 1",
		"limits.maxStreamsPerResponse must be >= 1",
		"limits.aggregationScanLimit must be >= 0",
		"limits.maxMemoryMB must be >= 1",
		"limits.maxQueryRangeHours must be >= 1",
		`limits.defaultLimit (2) must be <= limits.maxLimit (1)`,
		`log.level must be one of debug|info|warn|error, got "verbose"`,
		`log.format must be one of json|text, got "xml"`,
	} {
		if !strings.Contains(errStr, want) {
			t.Errorf("validate() error missing %q in %q", want, errStr)
		}
	}
}

func TestApplyEnvCreatesBasicAuthLazily(t *testing.T) {
	raw := defaultRaw()
	raw.VLogs.BasicAuth = nil

	t.Setenv("PROXY_VLOGS_BASICAUTH_USERNAME", "alice")
	t.Setenv("PROXY_VLOGS_BASICAUTH_PASSWORD", "pw")

	applyEnv(raw)

	if raw.VLogs.BasicAuth == nil {
		t.Fatal("expected BasicAuth to be initialized")
	}
	if raw.VLogs.BasicAuth.Username != "alice" {
		t.Errorf("Username = %q, want %q", raw.VLogs.BasicAuth.Username, "alice")
	}
	if raw.VLogs.BasicAuth.Password != "pw" {
		t.Errorf("Password = %q, want %q", raw.VLogs.BasicAuth.Password, "pw")
	}
}

func TestApplyEnvSetsAggregationScanLimit(t *testing.T) {
	raw := defaultRaw()

	t.Setenv("PROXY_LIMITS_AGGREGATIONSCANLIMIT", "321")

	applyEnv(raw)

	if raw.Limits.AggregationScanLimit != 321 {
		t.Fatalf("AggregationScanLimit = %d, want 321", raw.Limits.AggregationScanLimit)
	}
}

func TestResolvePasswordFileNoopWithoutBasicAuth(t *testing.T) {
	cfg := &Config{}
	if err := resolvePasswordFile(cfg); err != nil {
		t.Fatalf("resolvePasswordFile() returned unexpected error: %v", err)
	}
}
