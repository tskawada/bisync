package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Node      NodeConfig       `yaml:"node"`
	Peer      PeerConfig       `yaml:"peer"`
	SyncPairs []SyncPairConfig `yaml:"sync_pairs"`
	Watcher   WatcherConfig    `yaml:"watcher"`
	Transfer  TransferConfig   `yaml:"transfer"`
	Conflict  ConflictConfig   `yaml:"conflict"`
	Recovery  RecoveryConfig   `yaml:"recovery"`
	Changelog ChangelogConfig  `yaml:"changelog"`
	Notify    NotifyConfig     `yaml:"notify"`
	Logging   LoggingConfig    `yaml:"logging"`
}

type NodeConfig struct {
	Name string `yaml:"name"`
}

type PeerConfig struct {
	Name     string `yaml:"name"`
	Address  string `yaml:"address"`
	SSHPort  int    `yaml:"ssh_port"`
	SSHUser  string `yaml:"ssh_user"`
	SSHKey   string `yaml:"ssh_key"`
	GRPCPort int    `yaml:"grpc_port"`

	// GRPCListen is the gRPC bind address; empty means every interface.
	GRPCListen string `yaml:"grpc_listen"`

	// SharedSecretFile holds a secret shared by both peers.
	SharedSecretFile string `yaml:"shared_secret_file"`

	// SharedSecret comes from BISYNC_SHARED_SECRET or SharedSecretFile, never
	// from the config file itself.
	SharedSecret string `yaml:"-"`
}

// ListenAddr returns the gRPC bind address. A bare host in grpc_listen gets
// grpc_port appended.
func (p PeerConfig) ListenAddr() string {
	if p.GRPCListen == "" {
		return fmt.Sprintf(":%d", p.GRPCPort)
	}
	if _, _, err := net.SplitHostPort(p.GRPCListen); err != nil {
		return net.JoinHostPort(p.GRPCListen, strconv.Itoa(p.GRPCPort))
	}
	return p.GRPCListen
}

// ListensOnAllInterfaces reports whether the server binds every interface.
func (p PeerConfig) ListensOnAllInterfaces() bool {
	host, _, err := net.SplitHostPort(p.ListenAddr())
	if err != nil {
		return true
	}
	return host == "" || host == "0.0.0.0" || host == "::"
}

type SyncPairConfig struct {
	Name            string   `yaml:"name"`
	LocalPath       string   `yaml:"local_path"`
	RemotePath      string   `yaml:"remote_path"`
	Excludes        []string `yaml:"excludes"`
	DebounceSeconds int      `yaml:"debounce_seconds"`
}

type WatcherConfig struct {
	DebounceSeconds int  `yaml:"debounce_seconds"`
	ExcludeSelf     bool `yaml:"exclude_self"`
}

type TransferConfig struct {
	MaxConcurrent  int      `yaml:"max_concurrent"`
	TimeoutSeconds int      `yaml:"timeout_seconds"`
	RsyncOptions   []string `yaml:"rsync_options"`
	BandwidthLimit int      `yaml:"bandwidth_limit"`
}

type ConflictConfig struct {
	Policy                 string `yaml:"policy"`
	ConflictDir            string `yaml:"conflict_dir"`
	TombstoneRetentionDays int    `yaml:"tombstone_retention_days"`
}

type RecoveryConfig struct {
	CatchupScan    bool `yaml:"catchup_scan"`
	CatchupWorkers int  `yaml:"catchup_workers"`
}

type ChangelogConfig struct {
	DBPath        string `yaml:"db_path"`
	WALMode       bool   `yaml:"wal_mode"`
	RetentionDays int    `yaml:"retention_days"`
}

type NotifyConfig struct {
	Handlers []NotifyHandlerConfig `yaml:"handlers"`
}

type NotifyHandlerConfig struct {
	Type    string   `yaml:"type"`
	URL     string   `yaml:"url"`
	Command string   `yaml:"command"`
	Events  []string `yaml:"events"`
}

type LoggingConfig struct {
	Level      string `yaml:"level"`
	Output     string `yaml:"output"`
	FilePath   string `yaml:"file_path"`
	MaxSizeMB  int    `yaml:"max_size_mb"`
	MaxBackups int    `yaml:"max_backups"`
}

var validConflictPolicies = map[string]bool{
	"lww":        true,
	"alpha_wins": true,
	"keep_both":  true,
	"manual":     true,
}

// Load reads and validates the config file at the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	cfg := defaults()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		Peer: PeerConfig{
			SSHPort:  22,
			GRPCPort: 50051,
		},
		Watcher: WatcherConfig{
			DebounceSeconds: 5,
			ExcludeSelf:     true,
		},
		Transfer: TransferConfig{
			MaxConcurrent:  4,
			TimeoutSeconds: 3600,
		},
		Conflict: ConflictConfig{
			ConflictDir:            ".bisync-conflicts",
			TombstoneRetentionDays: 30,
		},
		Recovery: RecoveryConfig{
			CatchupScan:    true,
			CatchupWorkers: 4,
		},
		Changelog: ChangelogConfig{
			DBPath:        "/var/lib/bisync/changelog.db",
			WALMode:       true,
			RetentionDays: 90,
		},
		Logging: LoggingConfig{
			Level:      "info",
			Output:     "stdout",
			MaxSizeMB:  100,
			MaxBackups: 5,
		},
	}
}

func (c *Config) applyDefaults() error {
	if c.Peer.SSHUser == "" {
		if u := os.Getenv("USER"); u != "" {
			c.Peer.SSHUser = u
		} else {
			c.Peer.SSHUser = "bisync"
		}
	}
	if c.Peer.SSHKey == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("get home dir: %w", err)
		}
		c.Peer.SSHKey = filepath.Join(home, ".ssh", "id_ed25519")
	}
	for i := range c.SyncPairs {
		if c.SyncPairs[i].DebounceSeconds == 0 {
			c.SyncPairs[i].DebounceSeconds = c.Watcher.DebounceSeconds
		}
	}
	return c.loadSharedSecret()
}

// loadSharedSecret populates Peer.SharedSecret. The environment wins so
// systemd can inject it via EnvironmentFile.
func (c *Config) loadSharedSecret() error {
	if s := strings.TrimSpace(os.Getenv("BISYNC_SHARED_SECRET")); s != "" {
		c.Peer.SharedSecret = s
		return nil
	}
	if c.Peer.SharedSecretFile == "" {
		return nil
	}
	info, err := os.Stat(c.Peer.SharedSecretFile)
	if err != nil {
		return fmt.Errorf("read peer.shared_secret_file: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("peer.shared_secret_file %q is readable by group or others (mode %#o); chmod 600 it",
			c.Peer.SharedSecretFile, info.Mode().Perm())
	}
	b, err := os.ReadFile(c.Peer.SharedSecretFile)
	if err != nil {
		return fmt.Errorf("read peer.shared_secret_file: %w", err)
	}
	secret := strings.TrimSpace(string(b))
	if secret == "" {
		return fmt.Errorf("peer.shared_secret_file %q is empty", c.Peer.SharedSecretFile)
	}
	c.Peer.SharedSecret = secret
	return nil
}

func (c *Config) validate() error {
	if c.Node.Name == "" {
		return fmt.Errorf("node.name is required")
	}
	if c.Peer.Name == "" {
		return fmt.Errorf("peer.name is required")
	}
	if c.Peer.Name == c.Node.Name {
		return fmt.Errorf("peer.name must differ from node.name")
	}
	if c.Peer.Address == "" {
		return fmt.Errorf("peer.address is required")
	}
	if len(c.SyncPairs) == 0 {
		return fmt.Errorf("sync_pairs must have at least one entry")
	}
	seen := make(map[string]string)
	for i, sp := range c.SyncPairs {
		if sp.LocalPath == "" {
			return fmt.Errorf("sync_pairs[%d].local_path is required", i)
		}
		info, err := os.Stat(sp.LocalPath)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("sync_pairs[%d].local_path %q must be an existing directory", i, sp.LocalPath)
		}
		abs, _ := filepath.Abs(sp.LocalPath)
		for prev, prevName := range seen {
			if abs == prev || strings.HasPrefix(abs, prev+"/") || strings.HasPrefix(prev, abs+"/") {
				return fmt.Errorf("sync_pairs[%d] local_path %q overlaps with %q", i, sp.LocalPath, prevName)
			}
		}
		seen[abs] = sp.Name
	}
	if !validConflictPolicies[c.Conflict.Policy] {
		return fmt.Errorf("conflict.policy must be one of: lww, alpha_wins, keep_both, manual")
	}
	for i, h := range c.Notify.Handlers {
		if h.Type != "webhook" && h.Type != "command" {
			return fmt.Errorf("notify.handlers[%d].type must be 'webhook' or 'command'", i)
		}
	}
	return nil
}

// SyncPairByName returns the sync pair config for the given name.
func (c *Config) SyncPairByName(name string) (SyncPairConfig, bool) {
	for _, sp := range c.SyncPairs {
		if sp.Name == name {
			return sp, true
		}
	}
	return SyncPairConfig{}, false
}
