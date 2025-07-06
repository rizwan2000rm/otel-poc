package internal

import (
	"database/sql"
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	arrowpb "github.com/open-telemetry/otel-arrow/api/experimental/arrow/v1"
)

func NewGRPCServer(cfg Config, db *sql.DB) (*grpc.Server, net.Listener) {
	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}
	grpcServer := grpc.NewServer()
	handler := NewArrowHandler(db)
	arrowpb.RegisterArrowTracesServiceServer(grpcServer, handler)
	arrowpb.RegisterArrowLogsServiceServer(grpcServer, handler)
	arrowpb.RegisterArrowMetricsServiceServer(grpcServer, handler)
	return grpcServer, lis
} 