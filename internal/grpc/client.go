package grpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxRetries   = 5
	retryBackoff = 5 * time.Second
)

// Client wraps a gRPC connection to a bisync peer.
type Client struct {
	conn   *grpc.ClientConn
	client BisyncServiceClient
	addr   string
}

// NewClient dials the peer at addr and returns a Client.
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %q: %w", addr, err)
	}
	return &Client{
		conn:   conn,
		client: newBisyncServiceClient(conn),
		addr:   addr,
	}, nil
}

// WaitReady blocks until the connection is ready or ctx is cancelled.
func (c *Client) WaitReady(ctx context.Context) error {
	for {
		state := c.conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !c.conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) ExchangeChangelog(ctx context.Context, req *ChangelogRequest) (*ChangelogResponse, error) {
	return c.client.ExchangeChangelog(ctx, req)
}

func (c *Client) AckTransfer(ctx context.Context, req *AckRequest) (*AckResponse, error) {
	return c.client.AckTransfer(ctx, req)
}

func (c *Client) DeleteFile(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	return c.client.DeleteFile(ctx, req)
}

func (c *Client) RenameFile(ctx context.Context, req *RenameRequest) (*RenameResponse, error) {
	return c.client.RenameFile(ctx, req)
}

func (c *Client) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	return c.client.Ping(ctx, req)
}

// bisyncServiceClientImpl is the generated-style client stub.
type bisyncServiceClientImpl struct {
	cc grpc.ClientConnInterface
}

func newBisyncServiceClient(cc grpc.ClientConnInterface) BisyncServiceClient {
	return &bisyncServiceClientImpl{cc: cc}
}

func (c *bisyncServiceClientImpl) ExchangeChangelog(ctx context.Context, req *ChangelogRequest, opts ...grpc.CallOption) (*ChangelogResponse, error) {
	resp := &ChangelogResponse{}
	if err := c.cc.Invoke(ctx, "/bisync.BisyncService/ExchangeChangelog", req, resp, opts...); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *bisyncServiceClientImpl) AckTransfer(ctx context.Context, req *AckRequest, opts ...grpc.CallOption) (*AckResponse, error) {
	resp := &AckResponse{}
	if err := c.cc.Invoke(ctx, "/bisync.BisyncService/AckTransfer", req, resp, opts...); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *bisyncServiceClientImpl) DeleteFile(ctx context.Context, req *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	resp := &DeleteResponse{}
	if err := c.cc.Invoke(ctx, "/bisync.BisyncService/DeleteFile", req, resp, opts...); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *bisyncServiceClientImpl) RenameFile(ctx context.Context, req *RenameRequest, opts ...grpc.CallOption) (*RenameResponse, error) {
	resp := &RenameResponse{}
	if err := c.cc.Invoke(ctx, "/bisync.BisyncService/RenameFile", req, resp, opts...); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *bisyncServiceClientImpl) Ping(ctx context.Context, req *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	resp := &PingResponse{}
	if err := c.cc.Invoke(ctx, "/bisync.BisyncService/Ping", req, resp, opts...); err != nil {
		return nil, err
	}
	return resp, nil
}

// DialWithRetry dials, retrying up to maxRetries times on failure.
func DialWithRetry(ctx context.Context, addr string) (*Client, error) {
	var (
		client *Client
		err    error
	)
	for i := 0; i < maxRetries; i++ {
		client, err = NewClient(addr)
		if err == nil {
			return client, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryBackoff):
		}
	}
	return nil, fmt.Errorf("failed to connect to %q after %d retries: %w", addr, maxRetries, err)
}
