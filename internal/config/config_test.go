package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidate_missingNodeName(t *testing.T) {
	cfg := defaults()
	cfg.Peer.Name = "tina"
	cfg.Peer.Address = "tina"
	cfg.Conflict.Policy = "lww"
	if err := cfg.validate(); err == nil || err.Error() != "node.name is required" {
		t.Errorf("expected node.name error, got %v", err)
	}
}

func TestValidate_peerSameAsNode(t *testing.T) {
	cfg := defaults()
	cfg.Node.Name = "albus"
	cfg.Peer.Name = "albus"
	cfg.Peer.Address = "albus"
	cfg.Conflict.Policy = "lww"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for peer.name == node.name")
	}
}

func TestValidate_invalidConflictPolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := defaults()
	cfg.Node.Name = "albus"
	cfg.Peer.Name = "tina"
	cfg.Peer.Address = "tina"
	cfg.SyncPairs = []SyncPairConfig{{Name: "test", LocalPath: dir, RemotePath: "/remote"}}
	cfg.Conflict.Policy = "invalid"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for invalid conflict policy")
	}
}

func TestLoad_valid(t *testing.T) {
	dir := t.TempDir()
	cfgContent := `
node:
  name: albus
peer:
  name: tina
  address: tina
sync_pairs:
  - name: media
    local_path: ` + dir + `
    remote_path: /remote/media
conflict:
  policy: lww
`
	f := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(f, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Node.Name != "albus" {
		t.Errorf("expected albus, got %s", cfg.Node.Name)
	}
	if cfg.Watcher.DebounceSeconds != 5 {
		t.Errorf("expected default debounce 5, got %d", cfg.Watcher.DebounceSeconds)
	}
}

func TestValidate_overlappingPaths(t *testing.T) {
	parent := t.TempDir()
	child := filepath.Join(parent, "sub")
	if err := os.Mkdir(child, 0755); err != nil {
		t.Fatal(err)
	}
	cfg := defaults()
	cfg.Node.Name = "albus"
	cfg.Peer.Name = "tina"
	cfg.Peer.Address = "tina"
	cfg.Conflict.Policy = "lww"
	cfg.SyncPairs = []SyncPairConfig{
		{Name: "a", LocalPath: parent, RemotePath: "/r/a"},
		{Name: "b", LocalPath: child, RemotePath: "/r/b"},
	}
	if err := cfg.validate(); err == nil {
		t.Error("expected overlap error")
	}
}
