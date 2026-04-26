package transfer

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tskawada/bisync/internal/config"
)

const (
	maxRetries    = 10
	retryBaseWait = 10 * time.Second
)

// PIDTracker is called when an rsync process starts/stops so the watcher can
// exclude its events.
type PIDTracker interface {
	RegisterRsyncPID(pid int)
	UnregisterRsyncPID(pid int)
}

// Manager handles file transfers via rsync over SSH.
type Manager struct {
	cfg        *config.Config
	pool       *pool
	pidTracker PIDTracker
}

// NewManager creates a transfer Manager.
func NewManager(cfg *config.Config, pidTracker PIDTracker) *Manager {
	return &Manager{
		cfg:        cfg,
		pool:       newPool(cfg.Transfer.MaxConcurrent),
		pidTracker: pidTracker,
	}
}

// Close shuts down the worker pool gracefully.
func (m *Manager) Close() { m.pool.close() }

// Send transfers the given relative paths within sp from local to peer.
func (m *Manager) Send(ctx context.Context, sp config.SyncPairConfig, relPaths []string) error {
	if len(relPaths) == 0 {
		return nil
	}

	listFile, err := writeTempFileList(relPaths)
	if err != nil {
		return fmt.Errorf("write file list: %w", err)
	}
	defer os.Remove(listFile)

	resultCh := m.pool.submit(ctx, func() error {
		return m.runRsync(ctx, sp, listFile)
	})

	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) runRsync(ctx context.Context, sp config.SyncPairConfig, listFile string) error {
	peer := m.cfg.Peer
	src := ensureTrailingSlash(sp.LocalPath)
	dst := fmt.Sprintf("%s@%s:%s/", peer.SSHUser, peer.Address, sp.RemotePath)

	args := buildRsyncArgs(m.cfg, listFile, src, dst)

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			wait := time.Duration(math.Min(retryBaseWait.Seconds()*math.Pow(2, float64(attempt-1)), 300)) * time.Second
			log.Info().Int("attempt", attempt).Dur("wait", wait).Msg("rsync: retrying")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		if err := m.execRsync(ctx, args); err != nil {
			lastErr = err
			log.Warn().Err(err).Int("attempt", attempt).Msg("rsync: failed")
			continue
		}
		return nil
	}
	return fmt.Errorf("rsync failed after %d retries: %w", maxRetries, lastErr)
}

func (m *Manager) execRsync(ctx context.Context, args []string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx,
		time.Duration(m.cfg.Transfer.TimeoutSeconds)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(timeoutCtx, "rsync", args...)
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("rsync start: %w", err)
	}

	pid := cmd.Process.Pid
	if m.pidTracker != nil {
		m.pidTracker.RegisterRsyncPID(pid)
		defer m.pidTracker.UnregisterRsyncPID(pid)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("rsync: %w", err)
	}
	return nil
}

func buildRsyncArgs(cfg *config.Config, listFile, src, dst string) []string {
	peer := cfg.Peer
	sshCmd := fmt.Sprintf("ssh -p %d -i %s -o StrictHostKeyChecking=accept-new",
		peer.SSHPort, peer.SSHKey)

	args := []string{
		"-avz",
		"--dirs",
		"--files-from=" + listFile,
		fmt.Sprintf("--timeout=%d", cfg.Transfer.TimeoutSeconds),
		"--partial",
		"--partial-dir=.bisync-partial",
		"-e", sshCmd,
	}
	if cfg.Transfer.BandwidthLimit > 0 {
		args = append(args, fmt.Sprintf("--bwlimit=%d", cfg.Transfer.BandwidthLimit))
	}
	args = append(args, cfg.Transfer.RsyncOptions...)
	args = append(args, src, dst)
	return args
}

func writeTempFileList(paths []string) (string, error) {
	f, err := os.CreateTemp("", "bisync-files-*")
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = f.WriteString(strings.Join(paths, "\n") + "\n")
	return f.Name(), err
}

func ensureTrailingSlash(p string) string {
	return filepath.Clean(p) + "/"
}
