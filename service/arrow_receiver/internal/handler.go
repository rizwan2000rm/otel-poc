package internal

import (
	"database/sql"
	"io"

	log "github.com/sirupsen/logrus"

	arrowpb "tonbo/arrow_receiver/gen"
)

type ArrowHandler struct {
	arrowpb.UnimplementedArrowTracesServiceServer
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
// Future: ArrowLogs, ArrowMetrics methods 