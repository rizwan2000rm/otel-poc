package internal

import (
	arrowpb "github.com/open-telemetry/otel-arrow/api/experimental/arrow/v1"

	arrowrecord "github.com/open-telemetry/otel-arrow/pkg/otel/arrow_record"
	plog "go.opentelemetry.io/collector/pdata/plog"
	pmetric "go.opentelemetry.io/collector/pdata/pmetric"
	ptrace "go.opentelemetry.io/collector/pdata/ptrace"
)

// ArrowToOtlpLogs converts Arrow BatchArrowRecords to OTLP plog.Logs using the official otel-arrow consumer.
// Returns the first Logs object in the batch (usually only one is present).
func ArrowToOtlpLogs(batch *arrowpb.BatchArrowRecords) (plog.Logs, error) {
	consumer := arrowrecord.NewConsumer()
	defer consumer.Close()
	logs, err := consumer.LogsFrom(batch)
	if err != nil {
		return plog.Logs{}, err
	}
	if len(logs) == 0 {
		return plog.Logs{}, nil
	}
	return logs[0], nil
}

// ArrowToOtlpMetrics converts Arrow BatchArrowRecords to OTLP pmetric.Metrics using the official otel-arrow consumer.
// Returns the first Metrics object in the batch (usually only one is present).
func ArrowToOtlpMetrics(batch *arrowpb.BatchArrowRecords) (pmetric.Metrics, error) {
	consumer := arrowrecord.NewConsumer()
	defer consumer.Close()
	metrics, err := consumer.MetricsFrom(batch)
	if err != nil {
		return pmetric.Metrics{}, err
	}
	if len(metrics) == 0 {
		return pmetric.Metrics{}, nil
	}
	return metrics[0], nil
}

// ArrowToOtlpTraces converts Arrow BatchArrowRecords to OTLP ptrace.Traces using the official otel-arrow consumer.
// Returns the first Traces object in the batch (usually only one is present).
func ArrowToOtlpTraces(batch *arrowpb.BatchArrowRecords) (ptrace.Traces, error) {
	consumer := arrowrecord.NewConsumer()
	defer consumer.Close()
	traces, err := consumer.TracesFrom(batch)
	if err != nil {
		return ptrace.Traces{}, err
	}
	if len(traces) == 0 {
		return ptrace.Traces{}, nil
	}
	return traces[0], nil
}
