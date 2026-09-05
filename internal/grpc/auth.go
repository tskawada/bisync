package grpc

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Metadata keys must be lowercase.
const authMetadataKey = "bisync-auth"

// serverAuthInterceptor rejects calls that do not carry the shared secret. It
// authenticates the caller; it does not protect the transport.
func serverAuthInterceptor(secret string) grpc.UnaryServerInterceptor {
	want := []byte(secret)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing peer credentials")
		}
		got := md.Get(authMetadataKey)
		if len(got) != 1 || subtle.ConstantTimeCompare([]byte(got[0]), want) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid peer credentials")
		}
		return handler(ctx, req)
	}
}

// clientAuthInterceptor attaches the shared secret to every outgoing call.
func clientAuthInterceptor(secret string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx = metadata.AppendToOutgoingContext(ctx, authMetadataKey, secret)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
