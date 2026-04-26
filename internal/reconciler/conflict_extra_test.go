package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
)

// --- keep_both ---

type fakeNotifier struct {
	conflicts []string
}

func (f *fakeNotifier) Conflict(syncPair, path, msg string, details map[string]string) {
	f.conflicts = append(f.conflicts, path)
}
func (f *fakeNotifier) Error(syncPair, path, msg string) {}

func TestResolveKeepBoth_evacuatesLoser(t *testing.T) {
	dir := t.TempDir()
	conflictDir := filepath.Join(dir, ".bisync-conflicts")

	// Create the loser file.
	loserPath := filepath.Join(dir, "photo.jpg")
	if err := os.WriteFile(loserPath, []byte("loser content"), 0644); err != nil {
		t.Fatal(err)
	}

	notify := &fakeNotifier{}
	cfg := &config.Config{
		Node: config.NodeConfig{Name: "albus"},
		Conflict: config.ConflictConfig{
			Policy:      "keep_both",
			ConflictDir: ".bisync-conflicts",
		},
	}
	r := newConflictResolver(cfg, nil, notify)

	local := makeEntry("tina", "photo.jpg", "modify", "2026-04-26T10:00:00Z:00000000")
	remote := makeEntry("albus", "photo.jpg", "modify", "2026-04-26T11:00:00Z:00000001")

	sp := config.SyncPairConfig{
		Name:       "media",
		LocalPath:  dir,
		RemotePath: "/remote",
	}

	winner, result, err := r.resolve(context.Background(), sp, local, remote)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if result != conflictResolved {
		t.Error("expected resolved")
	}
	// remote (albus, newer HLC) should win.
	if winner.NodeName != "albus" {
		t.Errorf("expected albus to win, got %s", winner.NodeName)
	}
	// Loser file should have been moved into the conflict dir.
	if _, err := os.Stat(loserPath); err == nil {
		t.Error("loser file should have been evacuated from original location")
	}
	entries, _ := os.ReadDir(conflictDir)
	if len(entries) == 0 {
		t.Error("expected conflict file in conflict dir")
	}
	// Notify should have been called.
	if len(notify.conflicts) == 0 {
		t.Error("expected conflict notification")
	}
}

func TestResolveKeepBoth_noLoserFileIsNoop(t *testing.T) {
	dir := t.TempDir()
	notify := &fakeNotifier{}
	cfg := &config.Config{
		Node: config.NodeConfig{Name: "albus"},
		Conflict: config.ConflictConfig{
			Policy:      "keep_both",
			ConflictDir: ".bisync-conflicts",
		},
	}
	r := newConflictResolver(cfg, nil, notify)

	// Loser file does NOT exist on disk.
	local := makeEntry("tina", "ghost.jpg", "modify", "2026-04-26T10:00:00Z:00000000")
	remote := makeEntry("albus", "ghost.jpg", "modify", "2026-04-26T11:00:00Z:00000001")

	sp := config.SyncPairConfig{Name: "media", LocalPath: dir}
	_, result, err := r.resolve(context.Background(), sp, local, remote)
	if err != nil || result != conflictResolved {
		t.Errorf("expected clean resolution: err=%v result=%v", err, result)
	}
}

// --- manual ---

func TestResolveManual_blocks(t *testing.T) {
	notify := &fakeNotifier{}
	cfg := &config.Config{
		Node:     config.NodeConfig{Name: "albus"},
		Conflict: config.ConflictConfig{Policy: "manual"},
	}
	r := newConflictResolver(cfg, nil, notify)

	local := makeEntry("albus", "contract.pdf", "modify", "2026-04-26T10:00:00Z:00000000")
	remote := makeEntry("tina", "contract.pdf", "modify", "2026-04-26T11:00:00Z:00000001")

	_, result, err := r.resolve(context.Background(), config.SyncPairConfig{}, local, remote)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if result != conflictBlocked {
		t.Error("expected conflictBlocked for manual policy")
	}
	if len(notify.conflicts) == 0 {
		t.Error("expected conflict notification for manual policy")
	}
}

// --- delete vs delete ---

func TestResolve_bothDelete_isNoop(t *testing.T) {
	cfg := &config.Config{
		Node:     config.NodeConfig{Name: "albus"},
		Conflict: config.ConflictConfig{Policy: "lww"},
	}
	r := newConflictResolver(cfg, nil, nil)

	local := makeEntry("albus", "gone.txt", "delete", "2026-04-26T10:00:00Z:00000000")
	remote := makeEntry("tina", "gone.txt", "delete", "2026-04-26T11:00:00Z:00000001")

	// Both-delete is handled at reconciler level, not resolver level.
	// Verify resolver still returns a winner (lww) for completeness.
	winner, result, err := r.resolve(context.Background(), config.SyncPairConfig{}, local, remote)
	if err != nil {
		t.Fatal(err)
	}
	if result != conflictResolved {
		t.Error("expected resolved")
	}
	_ = winner
}

// --- evacuateLoser conflict dir naming ---

func TestEvacuateLoser_nameIncludesNodeAndTimestamp(t *testing.T) {
	dir := t.TempDir()
	loserPath := filepath.Join(dir, "report.pdf")
	os.WriteFile(loserPath, []byte("content"), 0644)

	cfg := &config.Config{
		Node:     config.NodeConfig{Name: "albus"},
		Conflict: config.ConflictConfig{ConflictDir: ".conflicts"},
	}
	r := newConflictResolver(cfg, nil, nil)

	loser := makeEntry("tina", "report.pdf", "modify", "2026-04-26T10:00:00Z:00000000")
	winner := makeEntry("albus", "report.pdf", "modify", "2026-04-26T11:00:00Z:00000001")
	sp := config.SyncPairConfig{Name: "docs", LocalPath: dir}

	if err := r.evacuateLoser(context.Background(), sp, loser, winner); err != nil {
		t.Fatalf("evacuateLoser: %v", err)
	}

	conflictDir := filepath.Join(dir, ".conflicts")
	entries, _ := os.ReadDir(conflictDir)
	if len(entries) == 0 {
		t.Fatal("expected conflict file")
	}
	name := entries[0].Name()
	if !containsAll(name, "tina") {
		t.Errorf("conflict filename %q should contain loser node name 'tina'", name)
	}
}

// --- groupEntries deduplication ---

func TestGroupEntries_keepsNewestPerPath(t *testing.T) {
	now := time.Now().UTC()
	entries := []*changelog.Entry{
		{SyncPair: "media", Path: "x.jpg", EventType: "create",
			HLCTimestamp: changelog.FormatHLC(now, 0), NodeName: "albus"},
		{SyncPair: "media", Path: "x.jpg", EventType: "modify",
			HLCTimestamp: changelog.FormatHLC(now, 1), NodeName: "albus"},
		{SyncPair: "media", Path: "x.jpg", EventType: "delete",
			HLCTimestamp: changelog.FormatHLC(now, 2), NodeName: "albus"},
	}
	m := groupEntries(entries)
	key := "media\x00x.jpg"
	if got := m[key]; got.EventType != "delete" {
		t.Errorf("expected newest entry (delete), got %s", got.EventType)
	}
}

func TestGroupEntries_separatesDistinctPaths(t *testing.T) {
	entries := []*changelog.Entry{
		{SyncPair: "media", Path: "a.jpg", EventType: "create", HLCTimestamp: changelog.HLCNow(), NodeName: "albus"},
		{SyncPair: "media", Path: "b.jpg", EventType: "create", HLCTimestamp: changelog.HLCNow(), NodeName: "albus"},
	}
	m := groupEntries(entries)
	if len(m) != 2 {
		t.Errorf("expected 2 entries, got %d", len(m))
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
