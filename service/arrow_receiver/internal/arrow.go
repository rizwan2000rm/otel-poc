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
		scopeJSON, _ := json.Marshal(rs.ScopeSpans().At(0).Scope().Attributes().AsRaw())
		schemaURL := rs.SchemaUrl()
		sl := rs.ScopeSpans()
		for j := 0; j < sl.Len(); j++ {
			scope := sl.At(j)
			spans := scope.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				attrsBytes, _ := json.Marshal(span.Attributes().AsRaw())
				attrsJSON := string(attrsBytes)

				// Serialize events
				events := make([]map[string]interface{}, span.Events().Len())
				for ei := 0; ei < span.Events().Len(); ei++ {
					e := span.Events().At(ei)
					events[ei] = map[string]interface{}{
						"name": e.Name(),
						"time_unix_nano": e.Timestamp().String(),
						"attributes": e.Attributes().AsRaw(),
						"dropped_attributes_count": e.DroppedAttributesCount(),
					}
				}
				eventsJSON, _ := json.Marshal(events)

				// Serialize links
				links := make([]map[string]interface{}, span.Links().Len())
				for li := 0; li < span.Links().Len(); li++ {
					l := span.Links().At(li)
					links[li] = map[string]interface{}{
						"trace_id": l.TraceID().String(),
						"span_id": l.SpanID().String(),
						"trace_state": l.TraceState().AsRaw(),
						"attributes": l.Attributes().AsRaw(),
						"dropped_attributes_count": l.DroppedAttributesCount(),
					}
				}
				linksJSON, _ := json.Marshal(links)

				parentSpanID := span.ParentSpanID().String()
				kind := int(span.Kind())
				traceState := span.TraceState().AsRaw()
				status := span.Status()
				statusCode := int(status.Code())
				statusMessage := status.Message()
				droppedAttrs := int(span.DroppedAttributesCount())
				droppedEvents := int(span.DroppedEventsCount())
				droppedLinks := int(span.DroppedLinksCount())
				startTime := span.StartTimestamp().String()
				endTime := span.EndTimestamp().String()
				if err := InsertTraceRow(ctx, db,
					span.TraceID().String(),
					span.SpanID().String(),
					parentSpanID,
					span.Name(),
					kind,
					traceState,
					statusCode,
					statusMessage,
					string(resourceJSON),
					attrsJSON,
					startTime,
					endTime,
					droppedAttrs,
					droppedEvents,
					droppedLinks,
					string(eventsJSON),
					string(linksJSON),
					string(scopeJSON),
					schemaURL,
				); err != nil {
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
		scopeJSON, _ := json.Marshal(rs.ScopeLogs().At(0).Scope().Attributes().AsRaw())
		schemaURL := rs.SchemaUrl()
		sl := rs.ScopeLogs()
		for j := 0; j < sl.Len(); j++ {
			scope := sl.At(j)
			logRecords := scope.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				logrec := logRecords.At(k)
				attrsBytes, _ := json.Marshal(logrec.Attributes().AsRaw())
				attrsJSON := string(attrsBytes)
				logID := logrec.TraceID().String() + logrec.SpanID().String()
				timeUnixNano := logrec.Timestamp().String()
				observedTimeUnixNano := logrec.ObservedTimestamp().String()
				severityNumber := int(logrec.SeverityNumber())
				severityText := logrec.SeverityText()
				body := logrec.Body().AsString()
				droppedAttrs := int(logrec.DroppedAttributesCount())
				flags := int(logrec.Flags())
				traceID := logrec.TraceID().String()
				spanID := logrec.SpanID().String()
				if err := InsertLogRow(ctx, db,
					logID,
					string(resourceJSON),
					timeUnixNano,
					observedTimeUnixNano,
					severityNumber,
					severityText,
					body,
					attrsJSON,
					droppedAttrs,
					flags,
					traceID,
					spanID,
					string(scopeJSON),
					schemaURL,
				); err != nil {
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
		scopeJSON, _ := json.Marshal(rs.ScopeMetrics().At(0).Scope().Attributes().AsRaw())
		schemaURL := rs.SchemaUrl()
		metricsSlice := rs.ScopeMetrics().At(0).Metrics()
		for j := 0; j < metricsSlice.Len(); j++ {
			metric := metricsSlice.At(j)
			name := metric.Name()
			unit := metric.Unit()
			description := metric.Description()
			aggTemporality := 0
			isMonotonic := false
			attrsJSON := "{}"
			if metric.Type() == pmetric.MetricTypeSum {
				aggTemporality = int(metric.Sum().AggregationTemporality())
				isMonotonic = metric.Sum().IsMonotonic()
				dps := metric.Sum().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					dp := dps.At(l)
					value := ""
					switch dp.ValueType() {
					case pmetric.NumberDataPointValueTypeInt:
						value = strconv.FormatInt(dp.IntValue(), 10)
					case pmetric.NumberDataPointValueTypeDouble:
						value = strconv.FormatFloat(dp.DoubleValue(), 'f', -1, 64)
					}
					attrsBytes, _ := json.Marshal(dp.Attributes().AsRaw())
					attrsJSON = string(attrsBytes)
					if err := InsertMetricRow(ctx, db,
						string(resourceJSON),
						name,
						unit,
						description,
						dp.StartTimestamp().String(),
						dp.Timestamp().String(),
						value,
						aggTemporality,
						isMonotonic,
						attrsJSON,
						string(scopeJSON),
						schemaURL,
					); err != nil {
						log.WithError(err).Error("Error inserting metric row")
					}
				}
			} else if metric.Type() == pmetric.MetricTypeGauge {
				dps := metric.Gauge().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					dp := dps.At(l)
					value := ""
					switch dp.ValueType() {
					case pmetric.NumberDataPointValueTypeInt:
						value = strconv.FormatInt(dp.IntValue(), 10)
					case pmetric.NumberDataPointValueTypeDouble:
						value = strconv.FormatFloat(dp.DoubleValue(), 'f', -1, 64)
					}
					attrsBytes, _ := json.Marshal(dp.Attributes().AsRaw())
					attrsJSON = string(attrsBytes)
					if err := InsertMetricRow(ctx, db,
						string(resourceJSON),
						name,
						unit,
						description,
						dp.StartTimestamp().String(),
						dp.Timestamp().String(),
						value,
						aggTemporality,
						isMonotonic,
						attrsJSON,
						string(scopeJSON),
						schemaURL,
					); err != nil {
						log.WithError(err).Error("Error inserting metric row")
					}
				}
			}
			// Add support for other metric types as needed
		}
	}
	return nil
} 
