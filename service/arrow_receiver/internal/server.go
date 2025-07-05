package internal

import (
	"database/sql"
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	arrowpb "tonbo/arrow_receiver/gen"
)

func NewGRPCServer(cfg Config, db *sql.DB) (*grpc.Server, net.Listener) {
	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}
	grpcServer := grpc.NewServer()
	arrowpb.RegisterArrowTracesServiceServer(grpcServer, NewArrowHandler(db))
	return grpcServer, lis
} 