package main

import (
	manager "RouteFinder/SDM"
)

var _ = manager.SDM{}

func main() {
	/*lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	api.RegisterServiceDiscoveryManagerServer(grpcServer, &api.Server{
		Manager: api.NewSDM(10),
	})
	println("gRPC server listening on port 50051")

	config := manager.NewSDMConfig().
		WithService("localhost:50051").
		WithRetryCount(2).
		Build()
	_ = manager.NewManager(config)*/

	/*if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}*/
}
