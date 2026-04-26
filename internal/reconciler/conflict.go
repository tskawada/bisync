package reconciler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
)

type conflictResult int

const (
	conflictResolved conflictResult = iota
	conflictBlocked
)

// conflictResolver handles conflict resolution according to the configured policy.
type conflictResolver struct {
	cfg      *config.Config
	store    *changelog.Store
	nodeName string
	notify   conflictNotifier
}

type conflictNotifier interface {
	Conflict(syncPair, path, message string, details map[string]string)
}

func newConflictResolver(cfg *config.Config, store *changelog.Store, notify conflictNotifier) *conflictResolver {
	return &conflictResolver{
		cfg:      cfg,
		store:    store,
		nodeName: cfg.Node.Name,
		notify:   notify,
	}
}

// resolve handles a conflict between a local and remote entry for the same path.
// It returns the entry that should be transferred (winner) and indicates
// whether the conflict was resolved or blocked.
func (r *conflictResolver) resolve(
	ctx context.Context,
	sp config.SyncPairConfig,
	local, remote *changelog.Entry,
) (winner *changelog.Entry, res conflictResult, err error) {

	policy := r.cfg.Conflict.Policy

	// Special handling regardless of policy: delete vs. modify/create.
	if local.EventType == "delete" && remote.EventType != "delete" {
		// Data-preservation rule: modify/create wins over delete.
		log.Info().Str("path", local.Path).Msg("conflict: delete vs modify — keeping modified file")
		return remote, conflictResolved, nil
	}
	if remote.EventType == "delete" && local.EventType != "delete" {
		log.Info().Str("path", local.Path).Msg("conflict: modify vs delete — keeping modified file")
		return local, conflictResolved, nil
	}

	switch policy {
	case "lww":
		return r.resolveLWW(local, remote)
	case "alpha_wins":
		return r.resolveAlphaWins(local, remote)
	case "keep_both":
		var res2 conflictResult
		winner, res2, err = r.resolveLWW(local, remote)
		if err != nil || res2 != conflictResolved {
			return nil, 0, err
		}
		loser := remote
		if winner == remote {
			loser = local
		}
		if err := r.evacuateLoser(ctx, sp, loser, winner); err != nil {
			return nil, 0, err
		}
		return winner, conflictResolved, nil
	case "manual":
		r.blockForManual(sp, local, remote)
		return nil, conflictBlocked, nil
	}
	return nil, 0, fmt.Errorf("unknown conflict policy %q", policy)
}

func (r *conflictResolver) resolveLWW(local, remote *changelog.Entry) (*changelog.Entry, conflictResult, error) {
	if changelog.CompareHLC(local.HLCTimestamp, remote.HLCTimestamp) >= 0 {
		return local, conflictResolved, nil
	}
	return remote, conflictResolved, nil
}

func (r *conflictResolver) resolveAlphaWins(local, remote *changelog.Entry) (*changelog.Entry, conflictResult, error) {
	// alpha = lexicographically first node name
	if local.NodeName <= remote.NodeName {
		return local, conflictResolved, nil
	}
	return remote, conflictResolved, nil
}

func (r *conflictResolver) evacuateLoser(ctx context.Context, sp config.SyncPairConfig, loser, winner *changelog.Entry) error {
	loserPath := filepath.Join(sp.LocalPath, loser.Path)
	if _, err := os.Lstat(loserPath); os.IsNotExist(err) {
		return nil
	}

	ts := time.Now().UTC().Format("20060102T150405Z")
	ext := filepath.Ext(loser.Path)
	base := loser.Path[:len(loser.Path)-len(ext)]
	conflictName := fmt.Sprintf("%s.%s.%s%s", base, loser.NodeName, ts, ext)
	conflictPath := filepath.Join(sp.LocalPath, r.cfg.Conflict.ConflictDir, conflictName)

	if err := os.MkdirAll(filepath.Dir(conflictPath), 0755); err != nil {
		return fmt.Errorf("mkdir conflict dir: %w", err)
	}
	if err := os.Rename(loserPath, conflictPath); err != nil {
		return fmt.Errorf("evacuate loser: %w", err)
	}

	log.Info().
		Str("original", loser.Path).
		Str("conflict", conflictName).
		Str("winner_node", winner.NodeName).
		Msg("conflict: file evacuated to conflict dir")

	if r.notify != nil {
		r.notify.Conflict(sp.Name, loser.Path,
			fmt.Sprintf("Conflict resolved (keep_both): winner=%s", winner.NodeName),
			map[string]string{
				"policy":        "keep_both",
				"winner_node":   winner.NodeName,
				"loser_node":    loser.NodeName,
				"conflict_file": conflictName,
			},
		)
	}
	return nil
}

func (r *conflictResolver) blockForManual(sp config.SyncPairConfig, local, remote *changelog.Entry) {
	log.Warn().
		Str("pair", sp.Name).
		Str("path", local.Path).
		Str("local_node", local.NodeName).
		Str("remote_node", remote.NodeName).
		Msg("conflict: manual policy — sync blocked for path")

	if r.notify != nil {
		r.notify.Conflict(sp.Name, local.Path,
			"Conflict detected: manual resolution required",
			map[string]string{
				"local_hlc":  local.HLCTimestamp,
				"remote_hlc": remote.HLCTimestamp,
				"policy":     "manual",
			},
		)
	}
}
