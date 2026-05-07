package reconciler

import (
	"context"
	"testing"

	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
)

func makeEntry(node, path, evType, hlc string) *changelog.Entry {
	return &changelog.Entry{
		SyncPair:     "media",
		Path:         path,
		EventType:    evType,
		HLCTimestamp: hlc,
		NodeName:     node,
	}
}

func TestResolveLWW_localWins(t *testing.T) {
	cfg := &config.Config{
		Node:     config.NodeConfig{Name: "albus"},
		Conflict: config.ConflictConfig{Policy: "lww"},
	}
	r := newConflictResolver(cfg, nil, nil)

	local := makeEntry("albus", "a.txt", "modify", "2026-04-26T12:00:01Z:00000001")
	remote := makeEntry("tina", "a.txt", "modify", "2026-04-26T12:00:00Z:00000000")

	winner, result, err := r.resolveLWW(local, remote)
	if err != nil {
		t.Fatal(err)
	}
	if result != conflictResolved {
		t.Error("expected resolved")
	}
	if winner != local {
		t.Error("expected local to win (newer HLC)")
	}
}

func TestResolveLWW_remoteWins(t *testing.T) {
	cfg := &config.Config{
		Node:     config.NodeConfig{Name: "albus"},
		Conflict: config.ConflictConfig{Policy: "lww"},
	}
	r := newConflictResolver(cfg, nil, nil)

	local := makeEntry("albus", "b.txt", "modify", "2026-04-26T10:00:00Z:00000000")
	remote := makeEntry("tina", "b.txt", "modify", "2026-04-26T11:00:00Z:00000005")

	winner, _, _ := r.resolveLWW(local, remote)
	if winner != remote {
		t.Error("expected remote to win (newer HLC)")
	}
}

func TestResolveAlphaWins(t *testing.T) {
	cfg := &config.Config{
		Node:     config.NodeConfig{Name: "albus"},
		Conflict: config.ConflictConfig{Policy: "alpha_wins"},
	}
	r := newConflictResolver(cfg, nil, nil)

	local := makeEntry("albus", "c.txt", "modify", "2026-04-26T10:00:00Z:00000000")
	remote := makeEntry("tina", "c.txt", "modify", "2026-04-26T11:00:00Z:00000000")

	// "albus" < "tina" lexicographically, so local wins
	winner, result, _ := r.resolveAlphaWins(local, remote)
	if result != conflictResolved {
		t.Error("expected resolved")
	}
	if winner != local {
		t.Error("expected albus (local) to win")
	}
}

func TestResolve_deleteVsModify(t *testing.T) {
	cfg := &config.Config{
		Node:     config.NodeConfig{Name: "albus"},
		Conflict: config.ConflictConfig{Policy: "lww"},
	}
	r := newConflictResolver(cfg, nil, nil)

	local := makeEntry("albus", "d.txt", "delete", "2026-04-26T10:00:00Z:00000000")
	remote := makeEntry("tina", "d.txt", "modify", "2026-04-26T09:00:00Z:00000000")

	winner, result, err := r.resolve(context.TODO(), config.SyncPairConfig{}, local, remote)
	if err != nil || result != conflictResolved {
		t.Fatalf("err=%v result=%v", err, result)
	}
	// modify wins over delete regardless of HLC
	if winner != remote {
		t.Error("expected remote (modify) to win over local (delete)")
	}
}

func TestGroupEntries_deduplicatesByHLC(t *testing.T) {
	older := makeEntry("albus", "x.txt", "create", "2026-04-26T10:00:00Z:00000000")
	newer := makeEntry("albus", "x.txt", "modify", "2026-04-26T11:00:00Z:00000001")

	m := groupEntries([]*changelog.Entry{older, newer})
	key := "media\x00x.txt"
	if got := m[key]; got != newer {
		t.Error("expected newer entry to win deduplication")
	}
}
