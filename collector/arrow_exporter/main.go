package main

import (
	"context"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	arrowpb "github.com/rizwan2000rm/arrow_exporter/proto"
)

type server struct {
	arrowpb.UnimplementedArrowServiceServer
}

func (s *server) Export(ctx context.Context, req *arrowpb.ExportArrowRequest) (*arrowpb.ExportArrowResponse, error) {
	f, err := os.OpenFile("arrow_data.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	f.Write(req.ArrowPayload)
	f.Write([]byte("\n---\n"))
	return &arrowpb.ExportArrowResponse{
		Status: &arrowpb.ExportStatus{
			Message: "ok",
			Code:    0,
		},
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	arrowpb.RegisterArrowServiceServer(grpcServer, &server{})
	log.Println("Arrow Exporter running on port 9000")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
} 