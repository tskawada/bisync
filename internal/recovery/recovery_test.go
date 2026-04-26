package recovery

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
)

func openStore(t *testing.T) *changelog.Store {
	t.Helper()
	s, err := changelog.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func makeCfg(localPath string) *config.Config {
	return &config.Config{
		Node: config.NodeConfig{Name: "albus"},
		SyncPairs: []config.SyncPairConfig{
			{Name: "media", LocalPath: localPath, RemotePath: "/remote/media"},
		},
		Recovery: config.RecoveryConfig{CatchupWorkers: 2},
	}
}

// TestRecordShutdown verifies the timestamp is persisted in metadata.
func TestRecordShutdown(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	m := New(makeCfg(t.TempDir()), store)

	before := time.Now().UTC().Add(-time.Second)
	if err := m.RecordShutdown(ctx); err != nil {
		t.Fatalf("RecordShutdown: %v", err)
	}
	after := time.Now().UTC().Add(time.Second)

	v, ok, err := store.GetMetadata(ctx, metaKeyLastShutdown)
	if err != nil || !ok {
		t.Fatalf("metadata not set: err=%v ok=%v", err, ok)
	}
	ts, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		t.Fatalf("parse timestamp: %v", err)
	}
	if ts.Before(before) || ts.After(after) {
		t.Errorf("timestamp %v out of expected range [%v, %v]", ts, before, after)
	}
}

// TestCatchupScan_firstBoot verifies that absence of last_shutdown is a no-op.
func TestCatchupScan_firstBoot(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte("hello"), 0644)

	m := New(makeCfg(dir), store)
	if err := m.CatchupScan(ctx); err != nil {
		t.Fatalf("CatchupScan: %v", err)
	}
	// No entries should have been written.
	entries, _ := store.UnsyncedEntries(ctx, "")
	if len(entries) != 0 {
		t.Errorf("expected 0 entries on first boot, got %d", len(entries))
	}
}

// TestCatchupScan_picksUpModifiedFiles verifies that files newer than
// last_shutdown are appended to the changelog.
func TestCatchupScan_picksUpModifiedFiles(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	dir := t.TempDir()

	// Record a shutdown time in the past.
	past := time.Now().UTC().Add(-2 * time.Second).Format(time.RFC3339Nano)
	store.SetMetadata(ctx, metaKeyLastShutdown, past)

	// Create a file that is newer than the last_shutdown.
	newFile := filepath.Join(dir, "new.txt")
	if err := os.WriteFile(newFile, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	m := New(makeCfg(dir), store)
	if err := m.CatchupScan(ctx); err != nil {
		t.Fatalf("CatchupScan: %v", err)
	}

	entries, err := store.UnsyncedEntries(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Error("expected at least one changelog entry for newly created file")
	}
	found := false
	for _, e := range entries {
		if e.Path == "new.txt" && e.EventType == "modify" && e.SyncPair == "media" {
			found = true
		}
	}
	if !found {
		t.Errorf("entry for 'new.txt' not found; got: %+v", entries)
	}
}

// TestCatchupScan_ignoresOldFiles verifies that files older than last_shutdown
// are not included.
func TestCatchupScan_ignoresOldFiles(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	dir := t.TempDir()

	// Create a file first.
	oldFile := filepath.Join(dir, "old.txt")
	os.WriteFile(oldFile, []byte("data"), 0644)

	// Set mtime to 1 hour ago.
	past := time.Now().UTC().Add(-1 * time.Hour)
	os.Chtimes(oldFile, past, past)

	// Record last_shutdown as 30 minutes ago (after the file's mtime).
	shutdown := time.Now().UTC().Add(-30 * time.Minute).Format(time.RFC3339Nano)
	store.SetMetadata(ctx, metaKeyLastShutdown, shutdown)

	m := New(makeCfg(dir), store)
	if err := m.CatchupScan(ctx); err != nil {
		t.Fatalf("CatchupScan: %v", err)
	}

	entries, _ := store.UnsyncedEntries(ctx, "")
	for _, e := range entries {
		if e.Path == "old.txt" {
			t.Errorf("old.txt should not be in changelog (mtime predates last_shutdown)")
		}
	}
}

// TestCatchupScan_multipleFiles verifies all qualifying files are scanned.
func TestCatchupScan_multipleFiles(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	dir := t.TempDir()

	past := time.Now().UTC().Add(-2 * time.Second).Format(time.RFC3339Nano)
	store.SetMetadata(ctx, metaKeyLastShutdown, past)

	sub := filepath.Join(dir, "sub")
	os.Mkdir(sub, 0755)
	for _, name := range []string{"a.txt", "b.txt", filepath.Join("sub", "c.txt")} {
		os.WriteFile(filepath.Join(dir, name), []byte(name), 0644)
	}

	m := New(makeCfg(dir), store)
	if err := m.CatchupScan(ctx); err != nil {
		t.Fatal(err)
	}

	entries, _ := store.UnsyncedEntries(ctx, "")
	paths := map[string]bool{}
	for _, e := range entries {
		paths[e.Path] = true
	}
	for _, want := range []string{"a.txt", "b.txt", filepath.Join("sub", "c.txt")} {
		if !paths[want] {
			t.Errorf("missing entry for %q; got %v", want, paths)
		}
	}
}

// TestCatchupScan_recordsNodeName verifies the node name is stored correctly.
func TestCatchupScan_recordsNodeName(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	dir := t.TempDir()

	past := time.Now().UTC().Add(-2 * time.Second).Format(time.RFC3339Nano)
	store.SetMetadata(ctx, metaKeyLastShutdown, past)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("x"), 0644)

	m := New(makeCfg(dir), store)
	m.CatchupScan(ctx)

	entries, _ := store.UnsyncedEntries(ctx, "")
	for _, e := range entries {
		if e.NodeName != "albus" {
			t.Errorf("expected node name 'albus', got %q", e.NodeName)
		}
	}
}
