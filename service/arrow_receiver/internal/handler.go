package internal

import (
	"database/sql"
	"io"

	log "github.com/sirupsen/logrus"

	arrowpb "github.com/open-telemetry/otel-arrow/api/experimental/arrow/v1"
)

type ArrowHandler struct {
	arrowpb.UnimplementedArrowTracesServiceServer
	arrowpb.UnimplementedArrowLogsServiceServer
	arrowpb.UnimplementedArrowMetricsServiceServer
	db *sql.DB
}

func NewArrowHandler(db *sql.DB) *ArrowHandler {
	return &ArrowHandler{db: db}
}

func (h *ArrowHandler) ArrowTraces(stream arrowpb.ArrowTracesService_ArrowTracesServer) error {
	ctx := stream.Context()
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			log.Info("Stream closed by client.")
			return nil
		}
		if err != nil {
			log.WithError(err).Error("Error receiving from stream")
			return err
		}
		log.WithField("record", record).Info("Received BatchArrowRecords")

		if err := ProcessTracesBatch(ctx, h.db, record); err != nil {
			log.WithError(err).Error("Error processing traces batch")
		}

		resp := &arrowpb.BatchStatus{
			BatchId:     record.BatchId,
			StatusCode:  arrowpb.StatusCode_OK,
			StatusMessage: "Received",
		}
		if err := stream.Send(resp); err != nil {
			log.WithError(err).Error("Error sending response")
			return err
		}
	}
}

func (h *ArrowHandler) ArrowLogs(stream arrowpb.ArrowLogsService_ArrowLogsServer) error {
	ctx := stream.Context()
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			log.Info("Logs stream closed by client.")
			return nil
		}
		if err != nil {
			log.WithError(err).Error("Error receiving logs from stream")
			return err
		}
		log.WithField("record", record).Info("Received BatchArrowRecords for logs")
		if err := ProcessLogsBatch(ctx, h.db, record); err != nil {
			log.WithError(err).Error("Error processing logs batch")
		}
		resp := &arrowpb.BatchStatus{
			BatchId:     record.BatchId,
			StatusCode:  arrowpb.StatusCode_OK,
			StatusMessage: "Received",
		}
		if err := stream.Send(resp); err != nil {
			log.WithError(err).Error("Error sending logs response")
			return err
		}
	}
}

func (h *ArrowHandler) ArrowMetrics(stream arrowpb.ArrowMetricsService_ArrowMetricsServer) error {
	ctx := stream.Context()
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			log.Info("Metrics stream closed by client.")
			return nil
		}
		if err != nil {
			log.WithError(err).Error("Error receiving metrics from stream")
			return err
		}
		log.WithField("record", record).Info("Received BatchArrowRecords for metrics")
		if err := ProcessMetricsBatch(ctx, h.db, record); err != nil {
			log.WithError(err).Error("Error processing metrics batch")
		}
		resp := &arrowpb.BatchStatus{
			BatchId:     record.BatchId,
			StatusCode:  arrowpb.StatusCode_OK,
			StatusMessage: "Received",
		}
		if err := stream.Send(resp); err != nil {
			log.WithError(err).Error("Error sending metrics response")
			return err
		}
	}
}