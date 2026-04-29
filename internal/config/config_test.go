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
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q, want %q", cfg.Log.Level, "debug")
	}
	if cfg.Log.Format != "text" {
		t.Errorf("Log.Format = %q, want %q", cfg.Log.Format, "text")
	}
	if cfg.Limits.MaxStreamsPerResponse != 5000 {
		t.Errorf("MaxStreamsPerResponse = %d, want default 5000", cfg.Limits.MaxStreamsPerResponse)
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

func TestValidateReturnsCombinedErrors(t *testing.T) {
	cfg := &Config{
		Limits: LimitsConfig{
			MaxConcurrentQueries:  0,
			MaxQueueDepth:         -1,
			MaxResponseBodyBytes:  0,
			MaxStreamsPerResponse: 0,
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

func TestResolvePasswordFileNoopWithoutBasicAuth(t *testing.T) {
	cfg := &Config{}
	if err := resolvePasswordFile(cfg); err != nil {
		t.Fatalf("resolvePasswordFile() returned unexpected error: %v", err)
	}
}
