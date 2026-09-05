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

// --- gRPC listen address ---

func TestPeerConfig_ListenAddr(t *testing.T) {
	cases := []struct {
		listen string
		port   int
		want   string
	}{
		{"", 50051, ":50051"},                           // unset: every interface
		{"100.64.0.1", 50051, "100.64.0.1:50051"},       // bare host gets the port
		{"100.64.0.1:60000", 50051, "100.64.0.1:60000"}, // explicit port wins
		{"127.0.0.1", 50051, "127.0.0.1:50051"},
		{"fd7a::1", 50051, "[fd7a::1]:50051"}, // IPv6 gets bracketed
	}
	for _, c := range cases {
		p := PeerConfig{GRPCListen: c.listen, GRPCPort: c.port}
		if got := p.ListenAddr(); got != c.want {
			t.Errorf("ListenAddr(%q, %d) = %q, want %q", c.listen, c.port, got, c.want)
		}
	}
}

func TestPeerConfig_ListensOnAllInterfaces(t *testing.T) {
	cases := map[string]bool{
		"":           true,
		"0.0.0.0":    true,
		"::":         true,
		"100.64.0.1": false,
		"127.0.0.1":  false,
	}
	for listen, want := range cases {
		p := PeerConfig{GRPCListen: listen, GRPCPort: 50051}
		if got := p.ListensOnAllInterfaces(); got != want {
			t.Errorf("ListensOnAllInterfaces(%q) = %v, want %v", listen, got, want)
		}
	}
}

// --- shared secret ---

func TestLoadSharedSecret_fromEnv(t *testing.T) {
	t.Setenv("BISYNC_SHARED_SECRET", "  from-env  ")

	c := &Config{}
	if err := c.loadSharedSecret(); err != nil {
		t.Fatal(err)
	}
	if c.Peer.SharedSecret != "from-env" {
		t.Errorf("got %q, want %q (trimmed)", c.Peer.SharedSecret, "from-env")
	}
}

func TestLoadSharedSecret_fromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("from-file\n"), 0600); err != nil {
		t.Fatal(err)
	}

	c := &Config{Peer: PeerConfig{SharedSecretFile: path}}
	if err := c.loadSharedSecret(); err != nil {
		t.Fatal(err)
	}
	if c.Peer.SharedSecret != "from-file" {
		t.Errorf("got %q, want %q", c.Peer.SharedSecret, "from-file")
	}
}

func TestLoadSharedSecret_envWinsOverFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("from-file"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BISYNC_SHARED_SECRET", "from-env")

	c := &Config{Peer: PeerConfig{SharedSecretFile: path}}
	if err := c.loadSharedSecret(); err != nil {
		t.Fatal(err)
	}
	if c.Peer.SharedSecret != "from-env" {
		t.Errorf("env should win, got %q", c.Peer.SharedSecret)
	}
}

func TestLoadSharedSecret_rejectsWorldReadableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("leaky"), 0644); err != nil {
		t.Fatal(err)
	}

	c := &Config{Peer: PeerConfig{SharedSecretFile: path}}
	if err := c.loadSharedSecret(); err == nil {
		t.Error("expected a permissions error for a world-readable secret file")
	}
}

func TestLoadSharedSecret_rejectsEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("\n  \n"), 0600); err != nil {
		t.Fatal(err)
	}

	c := &Config{Peer: PeerConfig{SharedSecretFile: path}}
	if err := c.loadSharedSecret(); err == nil {
		t.Error("expected an error for an empty secret file")
	}
}

func TestLoadSharedSecret_notSettableFromYAML(t *testing.T) {
	// The secret must never come from the config file — those live in git.
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "node:\n  name: albus\npeer:\n  name: tina\n  address: tina\n  shared_secret: oops\n" +
		"sync_pairs:\n  - name: media\n    local_path: " + dir + "\nconflict:\n  policy: lww\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Peer.SharedSecret != "" {
		t.Errorf("shared_secret must be ignored in YAML, got %q", cfg.Peer.SharedSecret)
	}
}

// --- peer TLS ---

// validTestConfig returns a config that passes validate(), so a test can change
// one field and check that field alone.
func validTestConfig(t *testing.T) *Config {
	t.Helper()
	cfg := defaults()
	cfg.Node.Name = "albus"
	cfg.Peer.Name = "tina"
	cfg.Peer.Address = "tina"
	cfg.Conflict.Policy = "lww"
	cfg.SyncPairs = []SyncPairConfig{{Name: "media", LocalPath: t.TempDir()}}
	return cfg
}

func TestValidate_rejectsPartialTLSConfig(t *testing.T) {
	cfg := validTestConfig(t)
	cfg.Peer.TLS = TLSConfig{CertFile: "/etc/bisync/node.crt"}

	if err := cfg.validate(); err == nil {
		t.Error("expected a half-filled peer.tls block to be rejected")
	}
}

func TestTLSConfig_Enabled(t *testing.T) {
	full := TLSConfig{CertFile: "a", KeyFile: "b", CAFile: "c"}
	if !full.Enabled() {
		t.Error("a complete TLS block should be enabled")
	}
	if (TLSConfig{}).Enabled() {
		t.Error("an empty TLS block should be disabled")
	}
	if (TLSConfig{CertFile: "a", KeyFile: "b"}).Enabled() {
		t.Error("a block missing the CA should not be enabled")
	}
}

func TestValidate_rejectsWorldReadableTLSKey(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "node.key")
	if err := os.WriteFile(key, []byte("key"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := validTestConfig(t)
	cfg.Peer.TLS = TLSConfig{CertFile: "c", KeyFile: key, CAFile: "ca"}

	if err := cfg.validate(); err == nil {
		t.Error("expected a world-readable TLS key to be rejected")
	}
}

func TestApplyDefaults_serverNameFallsBackToPeerName(t *testing.T) {
	cfg := &Config{Peer: PeerConfig{Name: "tina"}}
	if err := cfg.applyDefaults(); err != nil {
		t.Fatal(err)
	}
	if cfg.Peer.TLS.ServerName != "tina" {
		t.Errorf("got %q, want %q", cfg.Peer.TLS.ServerName, "tina")
	}
}
