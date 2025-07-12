package internal

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb"
)

func InitDB(path string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func EnsureTracesTableExists(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS traces (
		trace_id TEXT,
		span_id TEXT,
		parent_span_id TEXT,
		name TEXT,
		kind INT,
		trace_state TEXT,
		status_code INT,
		status_message TEXT,
		resource JSON,
		attributes JSON,
		start_time_unix_nano TEXT,
		end_time_unix_nano TEXT,
		dropped_attributes_count INT,
		dropped_events_count INT,
		dropped_links_count INT,
		events JSON,
		links JSON,
		scope JSON,
		schema_url TEXT
	)`)
	return err
}

func InsertTraceRow(ctx context.Context, db *sql.DB,
	traceID, spanID, parentSpanID, name string,
	kind int, traceState string, statusCode int, statusMessage string,
	resourceJSON, attrsJSON, startTime, endTime string,
	droppedAttrs, droppedEvents, droppedLinks int,
	eventsJSON, linksJSON, scopeJSON, schemaURL string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO traces (
		trace_id, span_id, parent_span_id, name, kind, trace_state, status_code, status_message, resource, attributes, start_time_unix_nano, end_time_unix_nano, dropped_attributes_count, dropped_events_count, dropped_links_count, events, links, scope, schema_url
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		traceID, spanID, parentSpanID, name, kind, traceState, statusCode, statusMessage, resourceJSON, attrsJSON, startTime, endTime, droppedAttrs, droppedEvents, droppedLinks, eventsJSON, linksJSON, scopeJSON, schemaURL)
	return err
}

func EnsureLogsTableExists(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS logs (
		log_id TEXT,
		resource JSON,
		time_unix_nano TEXT,
		observed_time_unix_nano TEXT,
		severity_number INT,
		severity_text TEXT,
		body TEXT,
		attributes JSON,
		dropped_attributes_count INT,
		flags INT,
		trace_id TEXT,
		span_id TEXT,
		scope JSON,
		schema_url TEXT
	)`)
	return err
}

func InsertLogRow(ctx context.Context, db *sql.DB,
	logID, resourceJSON, timeUnixNano, observedTimeUnixNano string,
	severityNumber int, severityText, body string, attrsJSON string,
	droppedAttrs, flags int, traceID, spanID, scopeJSON, schemaURL string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO logs (
		log_id, resource, time_unix_nano, observed_time_unix_nano, severity_number, severity_text, body, attributes, dropped_attributes_count, flags, trace_id, span_id, scope, schema_url
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		logID, resourceJSON, timeUnixNano, observedTimeUnixNano, severityNumber, severityText, body, attrsJSON, droppedAttrs, flags, traceID, spanID, scopeJSON, schemaURL)
	return err
}

func EnsureMetricsTableExists(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS metrics (
		resource JSON,
		name TEXT,
		unit TEXT,
		description TEXT,
		start_time_unix_nano TEXT,
		time_unix_nano TEXT,
		value TEXT,
		aggregation_temporality INT,
		is_monotonic BOOL,
		attributes JSON,
		scope JSON,
		schema_url TEXT
	)`)
	if err != nil {
		fmt.Printf("[DB DEBUG] Error creating metrics table: %v\n", err)
	} else {
		fmt.Println("[DB DEBUG] EnsureMetricsTableExists executed successfully.")
	}
	return err
}

func InsertMetricRow(ctx context.Context, db *sql.DB,
	resourceJSON, name, unit, description, startTime, time, value string,
	aggTemporality int, isMonotonic bool, attrsJSON, scopeJSON, schemaURL string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO metrics (
		resource, name, unit, description, start_time_unix_nano, time_unix_nano, value, aggregation_temporality, is_monotonic, attributes, scope, schema_url
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		resourceJSON, name, unit, description, startTime, time, value, aggTemporality, isMonotonic, attrsJSON, scopeJSON, schemaURL)
	return err
} 