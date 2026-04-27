package main

import (
	"context"
	"log/slog"
	"testing"
)

func TestBuildLoggerRespectsLevelThresholds(t *testing.T) {
	tests := []struct {
		level       string
		enabled     slog.Level
		disabled    slog.Level
	}{
		{level: "debug", enabled: slog.LevelDebug, disabled: slog.Level(-8)},
		{level: "warn", enabled: slog.LevelWarn, disabled: slog.LevelInfo},
		{level: "error", enabled: slog.LevelError, disabled: slog.LevelWarn},
		{level: "info", enabled: slog.LevelInfo, disabled: slog.LevelDebug},
		{level: "unexpected", enabled: slog.LevelInfo, disabled: slog.LevelDebug},
	}

	for _, tc := range tests {
		logger := buildLogger(tc.level, "json")
		if !logger.Enabled(context.Background(), tc.enabled) {
			t.Errorf("buildLogger(%q): expected level %v to be enabled", tc.level, tc.enabled)
		}
		if logger.Enabled(context.Background(), tc.disabled) {
			t.Errorf("buildLogger(%q): expected level %v to be disabled", tc.level, tc.disabled)
		}
	}
}
