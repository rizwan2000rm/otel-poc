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
		parent_id TEXT,
		name TEXT,
		kind TEXT,
		status TEXT,
		resource JSON,
		attributes JSON
	)`)
	return err
}

func InsertTraceRow(ctx context.Context, db *sql.DB, traceID, spanID, parentID, name, kind, status, resourceJSON, attrsJSON string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO traces (trace_id, span_id, parent_id, name, kind, status, resource, attributes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		traceID, spanID, parentID, name, kind, status, resourceJSON, attrsJSON)
	return err
}

func EnsureLogsTableExists(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS logs (
		log_id TEXT,
		resource JSON,
		time_unix_nano TEXT,
		severity_number TEXT,
		severity_text TEXT,
		body TEXT,
		attributes JSON
	)`)
	return err
}

func InsertLogRow(ctx context.Context, db *sql.DB, logID, resourceJSON, timeUnixNano, severityNumber, severityText, body, attrsJSON string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO logs (log_id, resource, time_unix_nano, severity_number, severity_text, body, attributes) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		logID, resourceJSON, timeUnixNano, severityNumber, severityText, body, attrsJSON)
	return err
}

func EnsureMetricsTableExists(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS metrics (
		resource JSON,
		name TEXT,
		unit TEXT,
		start_time_unix_nano TEXT,
		time_unix_nano TEXT,
		value TEXT
	)`)
	if err != nil {
		fmt.Printf("[DB DEBUG] Error creating metrics table: %v\n", err)
	} else {
		fmt.Println("[DB DEBUG] EnsureMetricsTableExists executed successfully.")
	}
	return err
}

func InsertMetricRow(ctx context.Context, db *sql.DB, resourceJSON, name, unit, startTime, time, value string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO metrics (resource, name, unit, start_time_unix_nano, time_unix_nano, value) VALUES (?, ?, ?, ?, ?, ?)`,
		resourceJSON, name, unit, startTime, time, value)
	if err != nil {
		fmt.Printf("[DB DEBUG] Error inserting metric row: resource=%s, name=%s, unit=%s, startTime=%s, time=%s, value=%s, err=%v\n", resourceJSON, name, unit, startTime, time, value, err)
	} else {
		fmt.Printf("[DB DEBUG] InsertMetricRow succeeded: resource=%s, name=%s, unit=%s, startTime=%s, time=%s, value=%s\n", resourceJSON, name, unit, startTime, time, value)
	}
	return err
} 