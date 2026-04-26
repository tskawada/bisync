package grpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

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

	fullPath := filepath.Join(sp.LocalPath, req.Path)
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

	from := filepath.Join(sp.LocalPath, req.FromPath)
	to := filepath.Join(sp.LocalPath, req.ToPath)

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
