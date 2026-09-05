package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
	googlegrpc "google.golang.org/grpc"
)

// testCA issues certificates for the TLS tests.
type testCA struct {
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	pemDir string
	caPath string
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bisync-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	writePEM(t, caPath, "CERTIFICATE", der)

	return &testCA{cert: cert, key: key, pemDir: dir, caPath: caPath}
}

// issue writes a cert/key pair for name and returns a usable TLSConfig.
func (ca *testCA) issue(t *testing.T, name string) config.TLSConfig {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, name+".crt")
	keyPath := filepath.Join(dir, name+".key")
	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)

	return config.TLSConfig{
		CertFile:   certPath,
		KeyFile:    keyPath,
		CAFile:     ca.caPath,
		ServerName: name,
	}
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	b := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

// startTLSTestServer serves with serverTLS and dials with clientTLS, so the two
// can be given mismatched material on purpose.
func startTLSTestServer(t *testing.T, serverTLS, clientTLS config.TLSConfig) *Client {
	t.Helper()

	store, err := changelog.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	srv, err := NewServer(&config.Config{
		Node: config.NodeConfig{Name: "albus"},
		Peer: config.PeerConfig{Name: "tina", TLS: serverTLS},
	}, store)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.grpcSrv.GracefulStop() })

	// Dial 127.0.0.1 while verifying the name in the certificate.
	creds, err := clientTLSCredentials(clientTLS, clientTLS.ServerName)
	if err != nil {
		t.Fatalf("client credentials: %v", err)
	}
	conn, err := googlegrpc.NewClient(lis.Addr().String(),
		googlegrpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return &Client{conn: conn, client: newBisyncServiceClient(conn), addr: lis.Addr().String()}
}

func TestTLS_mutualHandshakeSucceeds(t *testing.T) {
	ca := newTestCA(t)
	server := ca.issue(t, "albus")
	client := ca.issue(t, "tina")
	client.ServerName = "albus" // the client verifies the server's name

	c := startTLSTestServer(t, server, client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Ping(ctx, &PingRequest{NodeName: "tina"}); err != nil {
		t.Fatalf("Ping over mTLS: %v", err)
	}
}

func TestTLS_rejectsClientCertFromAnotherCA(t *testing.T) {
	ca := newTestCA(t)
	other := newTestCA(t)

	server := ca.issue(t, "albus")
	rogue := other.issue(t, "tina")
	rogue.CAFile = ca.caPath // trusts the real server, but its own cert is foreign
	rogue.ServerName = "albus"

	c := startTLSTestServer(t, server, rogue)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Ping(ctx, &PingRequest{NodeName: "tina"}); err == nil {
		t.Fatal("expected a client certificate from an unknown CA to be rejected")
	}
}

func TestTLS_rejectsWrongServerName(t *testing.T) {
	ca := newTestCA(t)
	server := ca.issue(t, "albus")
	client := ca.issue(t, "tina")
	client.ServerName = "someone-else"

	c := startTLSTestServer(t, server, client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Ping(ctx, &PingRequest{NodeName: "tina"}); err == nil {
		t.Fatal("expected a server name mismatch to be rejected")
	}
}

func TestTLS_credentialsRejectBadFiles(t *testing.T) {
	ca := newTestCA(t)
	good := ca.issue(t, "albus")

	missing := good
	missing.CertFile = filepath.Join(t.TempDir(), "absent.crt")
	if _, err := serverTLSCredentials(missing); err == nil {
		t.Error("expected an error for a missing certificate")
	}

	emptyCA := good
	emptyCA.CAFile = filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(emptyCA.CAFile, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := serverTLSCredentials(emptyCA); err == nil {
		t.Error("expected an error for a CA file with no certificates")
	}
}
