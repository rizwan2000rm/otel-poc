package main

import (
	"io"
	"log"

	"net"

	"google.golang.org/grpc"

	arrowpb "tonbo/arrow_receiver/gen"
)

type server struct {
	arrowpb.UnimplementedArrowTracesServiceServer
}

func (s *server) ArrowTraces(stream arrowpb.ArrowTracesService_ArrowTracesServer) error {
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			log.Println("Stream closed by client.")
			return nil
		}
		if err != nil {
			log.Printf("Error receiving from stream: %v", err)
			return err
		}
		log.Printf("Received BatchArrowRecords: %+v", record)
		// Optionally, send a BatchStatus response
		resp := &arrowpb.BatchStatus{
			BatchId:     record.BatchId,
			StatusCode:  arrowpb.StatusCode_OK,
			StatusMessage: "Received",
		}
		if err := stream.Send(resp); err != nil {
			log.Printf("Error sending response: %v", err)
			return err
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", ":9002")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	arrowpb.RegisterArrowTracesServiceServer(grpcServer, &server{})
	log.Println("ArrowTracesService gRPC server listening on :9002")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
} 