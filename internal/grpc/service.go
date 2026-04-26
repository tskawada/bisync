package grpc

import (
	"context"

	"google.golang.org/grpc"
)

// BisyncServiceServer is the interface a server must implement.
type BisyncServiceServer interface {
	ExchangeChangelog(context.Context, *ChangelogRequest) (*ChangelogResponse, error)
	AckTransfer(context.Context, *AckRequest) (*AckResponse, error)
	DeleteFile(context.Context, *DeleteRequest) (*DeleteResponse, error)
	RenameFile(context.Context, *RenameRequest) (*RenameResponse, error)
	Ping(context.Context, *PingRequest) (*PingResponse, error)
}

// BisyncServiceClient is the interface a client must implement.
type BisyncServiceClient interface {
	ExchangeChangelog(ctx context.Context, req *ChangelogRequest, opts ...grpc.CallOption) (*ChangelogResponse, error)
	AckTransfer(ctx context.Context, req *AckRequest, opts ...grpc.CallOption) (*AckResponse, error)
	DeleteFile(ctx context.Context, req *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
	RenameFile(ctx context.Context, req *RenameRequest, opts ...grpc.CallOption) (*RenameResponse, error)
	Ping(ctx context.Context, req *PingRequest, opts ...grpc.CallOption) (*PingResponse, error)
}

// RegisterBisyncServiceServer attaches a server implementation to a gRPC server.
func RegisterBisyncServiceServer(s *grpc.Server, srv BisyncServiceServer) {
	s.RegisterService(&bisyncServiceDesc, srv)
}

var bisyncServiceDesc = grpc.ServiceDesc{
	ServiceName: "bisync.BisyncService",
	HandlerType: (*BisyncServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "ExchangeChangelog", Handler: exchangeChangelogHandler},
		{MethodName: "AckTransfer", Handler: ackTransferHandler},
		{MethodName: "DeleteFile", Handler: deleteFileHandler},
		{MethodName: "RenameFile", Handler: renameFileHandler},
		{MethodName: "Ping", Handler: pingHandler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "bisync.proto",
}

func exchangeChangelogHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &ChangelogRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BisyncServiceServer).ExchangeChangelog(ctx, req)
	}
	return interceptor(ctx, req, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/bisync.BisyncService/ExchangeChangelog",
	}, func(ctx context.Context, req any) (any, error) {
		return srv.(BisyncServiceServer).ExchangeChangelog(ctx, req.(*ChangelogRequest))
	})
}

func ackTransferHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &AckRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BisyncServiceServer).AckTransfer(ctx, req)
	}
	return interceptor(ctx, req, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/bisync.BisyncService/AckTransfer",
	}, func(ctx context.Context, req any) (any, error) {
		return srv.(BisyncServiceServer).AckTransfer(ctx, req.(*AckRequest))
	})
}

func deleteFileHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &DeleteRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BisyncServiceServer).DeleteFile(ctx, req)
	}
	return interceptor(ctx, req, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/bisync.BisyncService/DeleteFile",
	}, func(ctx context.Context, req any) (any, error) {
		return srv.(BisyncServiceServer).DeleteFile(ctx, req.(*DeleteRequest))
	})
}

func renameFileHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &RenameRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BisyncServiceServer).RenameFile(ctx, req)
	}
	return interceptor(ctx, req, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/bisync.BisyncService/RenameFile",
	}, func(ctx context.Context, req any) (any, error) {
		return srv.(BisyncServiceServer).RenameFile(ctx, req.(*RenameRequest))
	})
}

func pingHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &PingRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BisyncServiceServer).Ping(ctx, req)
	}
	return interceptor(ctx, req, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/bisync.BisyncService/Ping",
	}, func(ctx context.Context, req any) (any, error) {
		return srv.(BisyncServiceServer).Ping(ctx, req.(*PingRequest))
	})
}
