package internal

import (
	"context"
	"encoding/json"
	"strconv"

	"database/sql"

	log "github.com/sirupsen/logrus"
	pmetric "go.opentelemetry.io/collector/pdata/pmetric"

	arrowpb "github.com/open-telemetry/otel-arrow/api/experimental/arrow/v1"
)

func ProcessTracesBatch(ctx context.Context, db *sql.DB, batch *arrowpb.BatchArrowRecords) error {
	traces, err := ArrowToOtlpTraces(batch)
	if err != nil {
		log.WithError(err).Error("Error converting Arrow to OTLP traces")
		return err
	}
	if err := EnsureTracesTableExists(ctx, db); err != nil {
		return err
	}
	// Save each span to DB
	rl := traces.ResourceSpans()
	for i := 0; i < rl.Len(); i++ {
		rs := rl.At(i)
		resourceJSON, _ := json.Marshal(rs.Resource().Attributes().AsRaw())
		sl := rs.ScopeSpans()
		for j := 0; j < sl.Len(); j++ {
			scope := sl.At(j)
			spans := scope.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				attrsJSON, _ := json.Marshal(span.Attributes().AsRaw())
				if err := InsertTraceRow(ctx, db, span.TraceID().String(), span.SpanID().String(), span.ParentSpanID().String(), span.Name(), span.Kind().String(), span.Status().Code().String(), string(resourceJSON), string(attrsJSON)); err != nil {
					log.WithError(err).Error("Error inserting trace row")
				}
			}
		}
	}
	return nil
}

func ProcessLogsBatch(ctx context.Context, db *sql.DB, batch *arrowpb.BatchArrowRecords) error {
	logs, err := ArrowToOtlpLogs(batch)
	if err != nil {
		log.WithError(err).Error("Error converting Arrow to OTLP logs")
		return err
	}
	if err := EnsureLogsTableExists(ctx, db); err != nil {
		return err
	}
	rl := logs.ResourceLogs()
	for i := 0; i < rl.Len(); i++ {
		rs := rl.At(i)
		resourceJSON, _ := json.Marshal(rs.Resource().Attributes().AsRaw())
		sl := rs.ScopeLogs()
		for j := 0; j < sl.Len(); j++ {
			scope := sl.At(j)
			logRecords := scope.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				logrec := logRecords.At(k)
				attrsJSON, _ := json.Marshal(logrec.Attributes().AsRaw())
				if err := InsertLogRow(ctx, db, logrec.TraceID().String()+logrec.SpanID().String(), string(resourceJSON), logrec.Timestamp().String(), logrec.SeverityNumber().String(), logrec.SeverityText(), logrec.Body().AsString(), string(attrsJSON)); err != nil {
					log.WithError(err).Error("Error inserting log row")
				}
			}
		}
	}
	return nil
}

func ProcessMetricsBatch(ctx context.Context, db *sql.DB, batch *arrowpb.BatchArrowRecords) error {
	metrics, err := ArrowToOtlpMetrics(batch)
	if err != nil {
		log.WithError(err).Error("Error converting Arrow to OTLP metrics")
		return err
	}
	if err := EnsureMetricsTableExists(ctx, db); err != nil {
		return err
	}
	rl := metrics.ResourceMetrics()
	for i := 0; i < rl.Len(); i++ {
		rs := rl.At(i)
		resourceJSON, _ := json.Marshal(rs.Resource().Attributes().AsRaw())
		sl := rs.ScopeMetrics()
		for j := 0; j < sl.Len(); j++ {
			scope := sl.At(j)
			metricsSlice := scope.Metrics()
			for k := 0; k < metricsSlice.Len(); k++ {
				metric := metricsSlice.At(k)
				if metric.Type() == pmetric.MetricTypeGauge {
					dps := metric.Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						var value string
						switch dp.ValueType() {
						case pmetric.NumberDataPointValueTypeInt:
							value = strconv.FormatInt(dp.IntValue(), 10)
						case pmetric.NumberDataPointValueTypeDouble:
							value = strconv.FormatFloat(dp.DoubleValue(), 'f', -1, 64)
						}
						if err := InsertMetricRow(ctx, db, string(resourceJSON), metric.Name(), metric.Unit(), dp.StartTimestamp().String(), dp.Timestamp().String(), value); err != nil {
							log.WithError(err).Error("Error inserting metric row")
						}
					}
				}
				// Add support for other metric types as needed
			}
		}
	}
	return nil
} 
