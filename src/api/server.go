package api

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	UnimplementedServiceDiscoveryManagerServer
}

func (s *Server) Ping(ctx context.Context, in *emptypb.Empty) (*PingReply, error) {
	return &PingReply{
		Message:   "pong",
		Timestamp: timestamppb.Now(),
	}, nil
}
