package grpc

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// startAuthTestServer starts a server configured with serverSecret and returns a
// client configured with clientSecret, so the two can be mismatched on purpose.
func startAuthTestServer(t *testing.T, serverSecret, clientSecret string) *Client {
	t.Helper()

	store, err := changelog.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	cfg := &config.Config{
		Node: config.NodeConfig{Name: "albus"},
		Peer: config.PeerConfig{Name: "tina", GRPCPort: 0, SharedSecret: serverSecret},
	}

	srv := NewServer(cfg, store)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.grpcSrv.GracefulStop() })

	opts := []googlegrpc.DialOption{
		googlegrpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if clientSecret != "" {
		opts = append(opts, googlegrpc.WithUnaryInterceptor(clientAuthInterceptor(clientSecret)))
	}
	conn, err := googlegrpc.NewClient(lis.Addr().String(), opts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &Client{conn: conn, client: newBisyncServiceClient(conn), addr: lis.Addr().String()}
}

func TestAuth_acceptsMatchingSecret(t *testing.T) {
	client := startAuthTestServer(t, "s3cret", "s3cret")

	if _, err := client.Ping(context.Background(), &PingRequest{NodeName: "tina"}); err != nil {
		t.Fatalf("Ping with matching secret: %v", err)
	}
}

func TestAuth_rejectsMissingAndWrongSecret(t *testing.T) {
	for name, clientSecret := range map[string]string{
		"no secret":    "",
		"wrong secret": "guess",
		// Same prefix — guards against a comparison that stops at the shorter
		// string.
		"prefix of the real secret": "s3c",
	} {
		t.Run(name, func(t *testing.T) {
			client := startAuthTestServer(t, "s3cret", clientSecret)

			_, err := client.Ping(context.Background(), &PingRequest{NodeName: "tina"})
			if err == nil {
				t.Fatal("expected Ping to be rejected")
			}
			if got := status.Code(err); got != codes.Unauthenticated {
				t.Errorf("expected Unauthenticated, got %v (%v)", got, err)
			}
		})
	}
}

func TestAuth_rejectsMutatingCallsWithoutSecret(t *testing.T) {
	client := startAuthTestServer(t, "s3cret", "")

	_, err := client.DeleteFile(context.Background(), &DeleteRequest{
		SyncPair: "media", Path: "x.txt", NodeName: "tina",
	})
	if err == nil {
		t.Fatal("expected DeleteFile to be rejected without credentials")
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v (%v)", got, err)
	}
}

func TestAuth_disabledWhenNoSecretConfigured(t *testing.T) {
	client := startAuthTestServer(t, "", "")

	if _, err := client.Ping(context.Background(), &PingRequest{NodeName: "tina"}); err != nil {
		t.Fatalf("Ping should succeed when auth is not configured: %v", err)
	}
}
