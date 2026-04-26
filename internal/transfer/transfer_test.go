package transfer

import (
	"strings"
	"testing"

	"github.com/tskawada/bisync/internal/config"
)

func minimalCfg() *config.Config {
	return &config.Config{
		Peer: config.PeerConfig{
			SSHPort: 22,
			SSHKey:  "/home/bisync/.ssh/id_ed25519",
			SSHUser: "bisync",
			Address: "tina",
		},
		Transfer: config.TransferConfig{
			TimeoutSeconds: 3600,
			BandwidthLimit: 0,
		},
	}
}

func TestBuildRsyncArgs_basic(t *testing.T) {
	cfg := minimalCfg()
	args := buildRsyncArgs(cfg, "/tmp/list", "/src/", "user@host:/dst/")
	joined := strings.Join(args, " ")

	for _, want := range []string{"-avz", "--files-from=/tmp/list", "--partial", "--partial-dir=.bisync-partial", "-e"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in args: %s", want, joined)
		}
	}
}

func TestBuildRsyncArgs_bandwidthLimit(t *testing.T) {
	cfg := minimalCfg()
	cfg.Transfer.BandwidthLimit = 1024
	args := buildRsyncArgs(cfg, "/tmp/list", "/src/", "user@host:/dst/")
	if !strings.Contains(strings.Join(args, " "), "--bwlimit=1024") {
		t.Errorf("missing --bwlimit in args")
	}
}

func TestWriteTempFileList(t *testing.T) {
	f, err := writeTempFileList([]string{"a/b.jpg", "c/d.pdf"})
	if err != nil {
		t.Fatal(err)
	}
	if f == "" {
		t.Error("expected non-empty temp file path")
	}
}
