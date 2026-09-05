package grpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/tskawada/bisync/internal/config"
	"google.golang.org/grpc/credentials"
)

// Both ends are this same binary, so nothing older needs to negotiate.
const minTLSVersion = tls.VersionTLS13

// serverTLSCredentials builds mutual-TLS credentials requiring a client
// certificate signed by the configured CA.
func serverTLSCredentials(t config.TLSConfig) (credentials.TransportCredentials, error) {
	cert, pool, err := loadKeyPairAndCA(t)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   minTLSVersion,
	}), nil
}

// clientTLSCredentials builds mutual-TLS credentials. serverName must match a
// SAN in the peer's certificate.
func clientTLSCredentials(t config.TLSConfig, serverName string) (credentials.TransportCredentials, error) {
	cert, pool, err := loadKeyPairAndCA(t)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   serverName,
		MinVersion:   minTLSVersion,
	}), nil
}

func loadKeyPairAndCA(t config.TLSConfig) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load tls key pair: %w", err)
	}
	ca, err := os.ReadFile(t.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("read tls ca file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return tls.Certificate{}, nil, fmt.Errorf("tls ca file %q contains no certificates", t.CAFile)
	}
	return cert, pool, nil
}
