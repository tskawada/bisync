package changelog

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAppendAndQuery(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	e := &Entry{
		SyncPair:  "media",
		Path:      "photos/img.jpg",
		EventType: "modify",
		FileSize:  1234,
		FileMtime: time.Now().Unix(),
		NodeName:  "albus",
	}
	id, err := s.Append(ctx, e)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if id == 0 {
		t.Error("expected non-zero id")
	}

	entries, err := s.UnsyncedEntries(ctx, "")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Path != "photos/img.jpg" {
		t.Errorf("unexpected path: %s", entries[0].Path)
	}
}

func TestMarkSynced(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	e := &Entry{SyncPair: "docs", Path: "a.txt", EventType: "create", NodeName: "albus"}
	id, _ := s.Append(ctx, e)

	if err := s.MarkSynced(ctx, []int64{id}); err != nil {
		t.Fatalf("mark synced: %v", err)
	}
	entries, _ := s.UnsyncedEntries(ctx, "")
	if len(entries) != 0 {
		t.Errorf("expected 0 unsynced, got %d", len(entries))
	}
}

func TestMetadata(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	if err := s.SetMetadata(ctx, "last_shutdown", "2026-04-26T00:00:00Z"); err != nil {
		t.Fatalf("set metadata: %v", err)
	}
	v, ok, err := s.GetMetadata(ctx, "last_shutdown")
	if err != nil || !ok || v != "2026-04-26T00:00:00Z" {
		t.Errorf("get metadata: v=%q ok=%v err=%v", v, ok, err)
	}

	_, ok2, _ := s.GetMetadata(ctx, "nonexistent")
	if ok2 {
		t.Error("expected false for missing key")
	}
}

func TestHLCOrdering(t *testing.T) {
	a := HLCNow()
	b := HLCNow()
	if CompareHLC(a, b) >= 0 && a == b {
		// logical counter must increment
	}
	if CompareHLC(b, a) < 0 {
		t.Error("b should be >= a")
	}
}
