package grpc

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// startTestServer spins up an in-process gRPC server and returns a connected client.
func startTestServer(t *testing.T) (*Client, *Server) {
	t.Helper()

	store, err := changelog.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	cfg := &config.Config{
		Node: config.NodeConfig{Name: "albus"},
		Peer: config.PeerConfig{Name: "tina", GRPCPort: 0},
	}

	srv := NewServer(cfg, store)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go srv.grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.grpcSrv.GracefulStop() })

	conn, err := googlegrpc.NewClient(lis.Addr().String(),
		googlegrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := &Client{
		conn:   conn,
		client: newBisyncServiceClient(conn),
		addr:   lis.Addr().String(),
	}
	return client, srv
}

// --- Ping ---

func TestPing_returnsNodeStatus(t *testing.T) {
	client, _ := startTestServer(t)

	resp, err := client.Ping(context.Background(), &PingRequest{NodeName: "tina"})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if resp.NodeName != "albus" {
		t.Errorf("expected node 'albus', got %q", resp.NodeName)
	}
	if resp.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", resp.Status)
	}
}

// --- ExchangeChangelog ---

func TestExchangeChangelog_emptyStore(t *testing.T) {
	client, _ := startTestServer(t)

	resp, err := client.ExchangeChangelog(context.Background(), &ChangelogRequest{})
	if err != nil {
		t.Fatalf("ExchangeChangelog: %v", err)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(resp.Entries))
	}
	if resp.CurrentHLC == "" {
		t.Error("expected non-empty CurrentHLC")
	}
}

func TestExchangeChangelog_returnsUnsyncedEntries(t *testing.T) {
	client, srv := startTestServer(t)
	ctx := context.Background()

	for _, path := range []string{"a.jpg", "b.jpg"} {
		srv.store.Append(ctx, &changelog.Entry{
			SyncPair: "media", Path: path, EventType: "modify", NodeName: "albus",
		})
	}

	resp, err := client.ExchangeChangelog(ctx, &ChangelogRequest{})
	if err != nil {
		t.Fatalf("ExchangeChangelog: %v", err)
	}
	if len(resp.Entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(resp.Entries))
	}
}

func TestExchangeChangelog_filtersBySyncPair(t *testing.T) {
	client, srv := startTestServer(t)
	ctx := context.Background()

	srv.store.Append(ctx, &changelog.Entry{SyncPair: "media", Path: "a.jpg", EventType: "modify", NodeName: "albus"})
	srv.store.Append(ctx, &changelog.Entry{SyncPair: "docs", Path: "b.pdf", EventType: "create", NodeName: "albus"})

	resp, err := client.ExchangeChangelog(ctx, &ChangelogRequest{SyncPair: "media"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Entries) != 1 || resp.Entries[0].SyncPair != "media" {
		t.Errorf("expected 1 media entry, got %+v", resp.Entries)
	}
}

func TestExchangeChangelog_filtersBySinceHLC(t *testing.T) {
	client, srv := startTestServer(t)
	ctx := context.Background()

	srv.store.Append(ctx, &changelog.Entry{SyncPair: "media", Path: "old.jpg", EventType: "modify", NodeName: "albus"})

	hlcBarrier := changelog.HLCNow()
	time.Sleep(time.Millisecond)

	srv.store.Append(ctx, &changelog.Entry{SyncPair: "media", Path: "new.jpg", EventType: "modify", NodeName: "albus"})

	resp, err := client.ExchangeChangelog(ctx, &ChangelogRequest{SinceHLC: hlcBarrier})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range resp.Entries {
		if e.Path == "old.jpg" {
			t.Error("old.jpg should be excluded by SinceHLC filter")
		}
	}
	found := false
	for _, e := range resp.Entries {
		if e.Path == "new.jpg" {
			found = true
		}
	}
	if !found {
		t.Error("new.jpg should be included after SinceHLC")
	}
}

// --- AckTransfer ---

func TestAckTransfer_marksPeerAck(t *testing.T) {
	client, srv := startTestServer(t)
	ctx := context.Background()

	id, _ := srv.store.Append(ctx, &changelog.Entry{
		SyncPair: "media", Path: "x.jpg", EventType: "modify", NodeName: "albus",
	})
	srv.store.MarkSynced(ctx, []int64{id})

	resp, err := client.AckTransfer(ctx, &AckRequest{EntryIDs: []int64{id}, NodeName: "tina"})
	if err != nil {
		t.Fatalf("AckTransfer: %v", err)
	}
	if !resp.Success {
		t.Error("expected success=true")
	}
}

// --- DeleteFile ---

func TestDeleteFile_removesFile(t *testing.T) {
	client, srv := startTestServer(t)
	ctx := context.Background()

	dir := t.TempDir()
	srv.cfg.SyncPairs = []config.SyncPairConfig{
		{Name: "media", LocalPath: dir, RemotePath: "/remote"},
	}

	target := filepath.Join(dir, "todelete.txt")
	if err := os.WriteFile(target, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	resp, err := client.DeleteFile(ctx, &DeleteRequest{
		SyncPair: "media", Path: "todelete.txt", NodeName: "tina",
	})
	if err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected success, got error: %s", resp.Error)
	}
	if _, err := os.Stat(target); err == nil {
		t.Error("file should have been deleted")
	}
}

func TestDeleteFile_rejectsUnknownSyncPair(t *testing.T) {
	client, _ := startTestServer(t)
	resp, err := client.DeleteFile(context.Background(), &DeleteRequest{
		SyncPair: "nonexistent", Path: "x.txt", NodeName: "tina",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Error("expected failure for unknown sync pair")
	}
}

func TestDeleteFile_succeedsWhenFileAlreadyAbsent(t *testing.T) {
	client, srv := startTestServer(t)
	dir := t.TempDir()
	srv.cfg.SyncPairs = []config.SyncPairConfig{
		{Name: "media", LocalPath: dir, RemotePath: "/remote"},
	}

	resp, err := client.DeleteFile(context.Background(), &DeleteRequest{
		SyncPair: "media", Path: "nonexistent.txt", NodeName: "tina",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Errorf("expected success for already-absent file, got: %s", resp.Error)
	}
}

// --- RenameFile ---

func TestRenameFile_renamesFile(t *testing.T) {
	client, srv := startTestServer(t)
	ctx := context.Background()

	dir := t.TempDir()
	srv.cfg.SyncPairs = []config.SyncPairConfig{
		{Name: "media", LocalPath: dir, RemotePath: "/remote"},
	}

	os.WriteFile(filepath.Join(dir, "before.txt"), []byte("content"), 0644)

	resp, err := client.RenameFile(ctx, &RenameRequest{
		SyncPair: "media", FromPath: "before.txt", ToPath: "after.txt", NodeName: "tina",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Errorf("expected success, got error: %s", resp.Error)
	}
	if _, err := os.Stat(filepath.Join(dir, "before.txt")); err == nil {
		t.Error("source should not exist after rename")
	}
	if _, err := os.Stat(filepath.Join(dir, "after.txt")); err != nil {
		t.Error("destination should exist after rename")
	}
}

func TestRenameFile_rejectsExistingTarget(t *testing.T) {
	client, srv := startTestServer(t)
	ctx := context.Background()

	dir := t.TempDir()
	srv.cfg.SyncPairs = []config.SyncPairConfig{
		{Name: "media", LocalPath: dir, RemotePath: "/remote"},
	}

	os.WriteFile(filepath.Join(dir, "src.txt"), []byte("a"), 0644)
	os.WriteFile(filepath.Join(dir, "dst.txt"), []byte("b"), 0644)

	resp, err := client.RenameFile(ctx, &RenameRequest{
		SyncPair: "media", FromPath: "src.txt", ToPath: "dst.txt", NodeName: "tina",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Error("expected failure when destination already exists")
	}
}

// --- JSON codec ---

func TestJSONCodec_roundTrip(t *testing.T) {
	c := jsonCodec{}
	original := &ChangelogEntry{
		ID:           42,
		SyncPair:     "media",
		Path:         "test/file.jpg",
		EventType:    "modify",
		HLCTimestamp: "2026-04-26T12:00:00Z:00000001",
		NodeName:     "albus",
		IsDir:        false,
	}
	data, err := c.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoded := &ChangelogEntry{}
	if err := c.Unmarshal(data, decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ID != 42 || decoded.Path != "test/file.jpg" || decoded.NodeName != "albus" {
		t.Errorf("round-trip mismatch: %+v", decoded)
	}
}

func TestJSONCodec_name(t *testing.T) {
	c := jsonCodec{}
	if c.Name() != "proto" {
		t.Error("codec name must be 'proto' to override gRPC default codec")
	}
}
