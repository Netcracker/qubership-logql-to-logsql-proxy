package vlogs

import (
	"testing"
	"time"
)

func TestNewMetadataCacheUsesDefaultSize(t *testing.T) {
	cache := NewMetadataCache(0)
	if cache.maxSize != 256 {
		t.Errorf("maxSize = %d, want 256", cache.maxSize)
	}
}

func TestMetadataCacheGetMissAndHit(t *testing.T) {
	cache := NewMetadataCache(2)
	if _, ok := cache.Get("missing"); ok {
		t.Fatal("expected missing key to miss")
	}

	cache.Set("k", []string{"v1", "v2"}, time.Minute)

	got, ok := cache.Get("k")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got) != 2 || got[0] != "v1" || got[1] != "v2" {
		t.Errorf("Get(k) = %v, want [v1 v2]", got)
	}
}

func TestMetadataCacheExpiresEntries(t *testing.T) {
	cache := NewMetadataCache(1)
	cache.Set("k", []string{"v"}, 20*time.Millisecond)

	time.Sleep(40 * time.Millisecond)

	if _, ok := cache.Get("k"); ok {
		t.Fatal("expected expired key to miss")
	}
}

func TestMetadataCacheEvictsExpiredBeforeLiveEntry(t *testing.T) {
	cache := NewMetadataCache(2)
	cache.Set("expired", []string{"old"}, 20*time.Millisecond)
	cache.Set("live", []string{"keep"}, time.Minute)

	time.Sleep(40 * time.Millisecond)
	cache.Set("new", []string{"fresh"}, time.Minute)

	if _, ok := cache.Get("expired"); ok {
		t.Fatal("expected expired entry to be evicted")
	}
	if _, ok := cache.Get("live"); !ok {
		t.Fatal("expected live entry to remain")
	}
	if _, ok := cache.Get("new"); !ok {
		t.Fatal("expected new entry to be present")
	}
}

func TestMetadataCacheEvictsArbitraryLiveEntryWhenFull(t *testing.T) {
	cache := NewMetadataCache(1)
	cache.Set("first", []string{"a"}, time.Minute)
	cache.Set("second", []string{"b"}, time.Minute)

	if len(cache.entries) != 1 {
		t.Fatalf("entry count = %d, want 1", len(cache.entries))
	}
	if _, ok := cache.Get("second"); !ok {
		t.Fatal("expected most recently inserted key to be present")
	}
}

func TestMetadataCacheKeyHelpersRoundToMinute(t *testing.T) {
	start := time.Date(2026, 4, 27, 10, 15, 45, 999, time.FixedZone("UTC+3", 3*3600))
	end := start.Add(20*time.Minute + 13*time.Second)

	if got, want := FieldNamesKey(start, end), "names:20260427T0715Z:20260427T0735Z"; got != want {
		t.Errorf("FieldNamesKey() = %q, want %q", got, want)
	}
	if got, want := FieldValuesKey("app", start, end), "values:app:20260427T0715Z:20260427T0735Z"; got != want {
		t.Errorf("FieldValuesKey() = %q, want %q", got, want)
	}
}
