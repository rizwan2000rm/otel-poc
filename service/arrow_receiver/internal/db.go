package internal

import (
	"context"
	"database/sql"

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