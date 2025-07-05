package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"tonbo/arrow_receiver/internal"
)

func main() {
	cfg := internal.LoadConfig()
	internal.SetupLogger()

	db, err := internal.InitDB(cfg.DBPath)
	if err != nil {
		log.WithError(err).Fatal("failed to open DuckDB")
	}
	defer db.Close()

	grpcServer, lis := internal.NewGRPCServer(cfg, db)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Info("Shutting down gRPC server...")
		grpcServer.GracefulStop()
		if lis != nil {
			_ = lis.Close()
		}
	}()

	log.WithFields(log.Fields{"port": cfg.GRPCPort}).Info("ArrowTracesService gRPC server listening")
	if err := grpcServer.Serve(lis); err != nil {
		log.WithError(err).Fatal("failed to serve")
	}
}
// Handler, server, arrow, and db logic moved to separate files. 