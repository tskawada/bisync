package grpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
	"google.golang.org/grpc"
)

// Server implements BisyncServiceServer and wraps a gRPC listener.
type Server struct {
	cfg      *config.Config
	store    *changelog.Store
	grpcSrv  *grpc.Server
	nodeName string
	status   string
}

// NewServer creates a new gRPC server.
func NewServer(cfg *config.Config, store *changelog.Store) *Server {
	s := &Server{
		cfg:      cfg,
		store:    store,
		nodeName: cfg.Node.Name,
		status:   "ok",
	}
	s.grpcSrv = grpc.NewServer()
	RegisterBisyncServiceServer(s.grpcSrv, s)
	return s
}

// Listen starts the gRPC server on the configured port and blocks until
// ctx is cancelled.
func (s *Server) Listen(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.Peer.GRPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	log.Info().Str("addr", addr).Msg("gRPC server listening")

	go func() {
		<-ctx.Done()
		s.grpcSrv.GracefulStop()
	}()

	return s.grpcSrv.Serve(lis)
}

// SetStatus updates the status string returned by Ping.
func (s *Server) SetStatus(status string) { s.status = status }

// ExchangeChangelog returns unsynced changelog entries to the requesting peer.
func (s *Server) ExchangeChangelog(ctx context.Context, req *ChangelogRequest) (*ChangelogResponse, error) {
	entries, err := s.store.EntriesSinceHLC(ctx, req.SinceHLC)
	if err != nil {
		return nil, fmt.Errorf("query changelog: %w", err)
	}

	var out []*ChangelogEntry
	for _, e := range entries {
		if req.SyncPair != "" && e.SyncPair != req.SyncPair {
			continue
		}
		out = append(out, entryToProto(e))
	}

	currentHLC, err := s.store.CurrentHLC(ctx)
	if err != nil {
		return nil, err
	}
	return &ChangelogResponse{Entries: out, CurrentHLC: currentHLC}, nil
}

// AckTransfer marks remote entries as peer_ack'd.
func (s *Server) AckTransfer(ctx context.Context, req *AckRequest) (*AckResponse, error) {
	if err := s.store.MarkPeerAck(ctx, req.EntryIDs); err != nil {
		return &AckResponse{Success: false}, err
	}
	return &AckResponse{Success: true}, nil
}

// DeleteFile deletes the specified file on this node.
func (s *Server) DeleteFile(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	sp, ok := s.cfg.SyncPairByName(req.SyncPair)
	if !ok {
		return &DeleteResponse{Success: false, Error: "unknown sync pair"}, nil
	}

	// Verify no local unsynced changes before deleting.
	hasUnsynced, err := s.store.HasUnsyncedForPath(ctx, req.SyncPair, req.Path)
	if err != nil {
		return &DeleteResponse{Success: false, Error: err.Error()}, nil
	}
	if hasUnsynced {
		return &DeleteResponse{Success: false, Error: "local unsynced changes exist"}, nil
	}

	fullPath, err := resolveInPair(sp.LocalPath, req.Path)
	if err != nil {
		log.Warn().Str("pair", req.SyncPair).Str("path", req.Path).Str("peer", req.NodeName).
			Err(err).Msg("DeleteFile: rejected path")
		return &DeleteResponse{Success: false, Error: err.Error()}, nil
	}
	if err := os.RemoveAll(fullPath); err != nil && !os.IsNotExist(err) {
		return &DeleteResponse{Success: false, Error: err.Error()}, nil
	}
	return &DeleteResponse{Success: true}, nil
}

// RenameFile renames the specified file on this node.
func (s *Server) RenameFile(ctx context.Context, req *RenameRequest) (*RenameResponse, error) {
	sp, ok := s.cfg.SyncPairByName(req.SyncPair)
	if !ok {
		return &RenameResponse{Success: false, Error: "unknown sync pair"}, nil
	}

	from, err := resolveInPair(sp.LocalPath, req.FromPath)
	if err != nil {
		log.Warn().Str("pair", req.SyncPair).Str("path", req.FromPath).Str("peer", req.NodeName).
			Err(err).Msg("RenameFile: rejected source path")
		return &RenameResponse{Success: false, Error: err.Error()}, nil
	}
	to, err := resolveInPair(sp.LocalPath, req.ToPath)
	if err != nil {
		log.Warn().Str("pair", req.SyncPair).Str("path", req.ToPath).Str("peer", req.NodeName).
			Err(err).Msg("RenameFile: rejected target path")
		return &RenameResponse{Success: false, Error: err.Error()}, nil
	}

	if err := os.MkdirAll(filepath.Dir(to), 0755); err != nil {
		return &RenameResponse{Success: false, Error: err.Error()}, nil
	}

	if _, err := os.Stat(to); err == nil {
		// Target already exists — caller should handle conflict first.
		return &RenameResponse{Success: false, Error: "rename target already exists"}, nil
	}

	if err := os.Rename(from, to); err != nil {
		return &RenameResponse{Success: false, Error: err.Error()}, nil
	}
	return &RenameResponse{Success: true}, nil
}

// Ping responds with node name and current sync status.
func (s *Server) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	return &PingResponse{NodeName: s.nodeName, Status: s.status}, nil
}

// resolveInPair resolves a peer-supplied relative path against a sync pair's
// local root and rejects anything that escapes it. filepath.Join alone is not
// enough: it cleans the result, so Join("/srv/data", "../../etc/x") yields
// "/etc/x".
func resolveInPair(localPath, rel string) (string, error) {
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("path must be relative")
	}
	// Rejected explicitly rather than left to the prefix check below.
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		if part == ".." {
			return "", fmt.Errorf("path must not contain %q", "..")
		}
	}
	if c := filepath.Clean(rel); c == "." || c == string(os.PathSeparator) {
		return "", fmt.Errorf("path must not be the sync pair root")
	}

	base := filepath.Clean(localPath)
	full := filepath.Join(base, rel)
	if !strings.HasPrefix(full, base+string(os.PathSeparator)) {
		return "", fmt.Errorf("path escapes sync pair root")
	}

	// A symlinked directory inside the tree can still point outside it. Walk up
	// to the deepest existing ancestor, since RenameFile's MkdirAll would
	// follow a link under a parent that does not yet exist.
	realBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		return "", fmt.Errorf("resolve sync pair root: %w", err)
	}
	for ancestor := filepath.Dir(full); ; {
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			if resolved != realBase && !strings.HasPrefix(resolved, realBase+string(os.PathSeparator)) {
				return "", fmt.Errorf("path escapes sync pair root via symlink")
			}
			return full, nil
		}
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("resolve %q: %w", ancestor, err)
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", fmt.Errorf("no existing ancestor under sync pair root")
		}
		ancestor = parent
	}
}

func entryToProto(e *changelog.Entry) *ChangelogEntry {
	return &ChangelogEntry{
		ID:           e.ID,
		SyncPair:     e.SyncPair,
		Path:         e.Path,
		EventType:    e.EventType,
		RenameTo:     e.RenameTo,
		FileSize:     e.FileSize,
		FileMtime:    e.FileMtime,
		FileMode:     e.FileMode,
		IsDir:        e.IsDir,
		HLCTimestamp: e.HLCTimestamp,
		NodeName:     e.NodeName,
	}
}
