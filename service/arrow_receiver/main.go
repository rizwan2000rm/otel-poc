package main

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"strconv"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"google.golang.org/grpc"

	arrowpb "tonbo/arrow_receiver/gen"
)

var db *sql.DB

type server struct {
	arrowpb.UnimplementedArrowTracesServiceServer
}

func (s *server) ArrowTraces(stream arrowpb.ArrowTracesService_ArrowTracesServer) error {
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			log.Println("Stream closed by client.")
			return nil
		}
		if err != nil {
			log.Printf("Error receiving from stream: %v", err)
			return err
		}
		log.Printf("Received BatchArrowRecords: %+v", record)

		if err := processTracesBatch(record); err != nil {
			log.Printf("Error processing traces batch: %v", err)
		}

		resp := &arrowpb.BatchStatus{
			BatchId:     record.BatchId,
			StatusCode:  arrowpb.StatusCode_OK,
			StatusMessage: "Received",
		}
		if err := stream.Send(resp); err != nil {
			log.Printf("Error sending response: %v", err)
			return err
		}
	}
}

func processTracesBatch(batch *arrowpb.BatchArrowRecords) error {
	pool := memory.NewGoAllocator()
	var (
		spansRec, spanAttrsRec, resourceRec arrow.Record
		spanAttrsMap = make(map[string]map[string]string) // span_id -> map[key]value
		resourceMap = make(map[string]map[string]string) // resource_id -> map[key]value
	)
	// 1. Parse all ArrowPayloads and group by type
	for _, payload := range batch.ArrowPayloads {
		rdr, err := ipc.NewReader(bytes.NewReader(payload.Record), ipc.WithAllocator(pool))
		if err != nil {
			log.Printf("Error decoding Arrow payload: %v", err)
			continue
		}
		for rdr.Next() {
			rec := rdr.Record()
			rec.Retain() // Retain the record so it stays valid after rdr.Release()
			switch payload.Type {
			case 40: // SPANS
				if spansRec != nil { spansRec.Release() }
				spansRec = rec
			case 41: // SPAN_ATTRS
				if spanAttrsRec != nil { spanAttrsRec.Release() }
				spanAttrsRec = rec
			case 1: // RESOURCE_ATTRS
				if resourceRec != nil { resourceRec.Release() }
				resourceRec = rec
			}
		}
		rdr.Release()
	}
	// Helper to get field names from an Arrow schema
	getFieldNames := func(schema *arrow.Schema) []string {
		names := make([]string, len(schema.Fields()))
		for i, f := range schema.Fields() {
			names[i] = f.Name
		}
		return names
	}
	// Print schema fields for debugging
	if spansRec != nil {
		log.Printf("SPANS schema: %v", getFieldNames(spansRec.Schema()))
		log.Printf("SPANS num rows: %d", spansRec.NumRows())
		log.Printf("SPANS columns: %+v", spansRec.Schema().Fields())
	}
	if resourceRec != nil {
		log.Printf("RESOURCE_ATTRS schema: %v", getFieldNames(resourceRec.Schema()))
		log.Printf("RESOURCE_ATTRS num rows: %d", resourceRec.NumRows())
		log.Printf("RESOURCE_ATTRS columns: %+v", resourceRec.Schema().Fields())
	}
	if spanAttrsRec != nil {
		log.Printf("SPAN_ATTRS schema: %v", getFieldNames(spanAttrsRec.Schema()))
		log.Printf("SPAN_ATTRS num rows: %d", spanAttrsRec.NumRows())
		log.Printf("SPAN_ATTRS columns: %+v", spanAttrsRec.Schema().Fields())
	}
	// 2. Build lookup maps for resource and span attributes
	if resourceRec != nil {
		for row := 0; row < int(resourceRec.NumRows()); row++ {
			id := ""
			// parent_id is uint16
			idIdx := -1
			for i, f := range resourceRec.Schema().Fields() {
				if f.Name == "parent_id" {
					idIdx = i
					break
				}
			}
			if idIdx != -1 {
				col := resourceRec.Column(idIdx)
				if arr, ok := col.(*array.Uint16); ok && arr.IsValid(row) {
					id = strconv.FormatUint(uint64(arr.Value(row)), 10)
				}
			}
			if id == "" { continue }
			if _, ok := resourceMap[id]; !ok {
				resourceMap[id] = make(map[string]string)
			}
			k := getString(resourceRec, "key", row)
			v := getString(resourceRec, "str", row)
			if k != "" && v != "" {
				resourceMap[id][k] = v
			}
		}
	}
	if spanAttrsRec != nil {
		for row := 0; row < int(spanAttrsRec.NumRows()); row++ {
			spanID := ""
			// parent_id is uint16
			idIdx := -1
			for i, f := range spanAttrsRec.Schema().Fields() {
				if f.Name == "parent_id" {
					idIdx = i
					break
				}
			}
			if idIdx != -1 {
				col := spanAttrsRec.Column(idIdx)
				if arr, ok := col.(*array.Uint16); ok && arr.IsValid(row) {
					spanID = strconv.FormatUint(uint64(arr.Value(row)), 10)
				}
			}
			if spanID == "" { continue }
			if _, ok := spanAttrsMap[spanID]; !ok {
				spanAttrsMap[spanID] = make(map[string]string)
			}
			k := getString(spanAttrsRec, "key", row)
			v := getString(spanAttrsRec, "str", row)
			if k != "" && v != "" {
				spanAttrsMap[spanID][k] = v
			}
		}
	}
	// 3. For each span, build a denormalized row
	if spansRec != nil {
		if err := ensureTracesTableExists(spansRec); err != nil {
			return err
		}
		for row := 0; row < int(spansRec.NumRows()); row++ {
			// Log all columns for this row
			for colIdx, field := range spansRec.Schema().Fields() {
				col := spansRec.Column(colIdx)
				log.Printf("Row %d, Col %s (%T): %v", row, field.Name, col, col)
			}
			traceID := getString(spansRec, "trace_id", row)
			spanID := getString(spansRec, "span_id", row)
			parentID := getString(spansRec, "parent_span_id", row)
			name := getString(spansRec, "name", row)
			kind := strconv.FormatInt(getInt(spansRec, "kind", row), 10)
			status := strconv.FormatInt(getInt(spansRec, "status", row), 10)

			// Extract resourceID from the resource struct column
			resourceID := ""
			resourceIdx := -1
			spanStructID := ""
			for i, f := range spansRec.Schema().Fields() {
				if f.Name == "resource" {
					resourceIdx = i
					break
				}
			}
			if resourceIdx != -1 {
				col := spansRec.Column(resourceIdx)
				if structArr, ok := col.(*array.Struct); ok {
					idFieldIdx := -1
					for i, f := range structArr.DataType().(*arrow.StructType).Fields() {
						if f.Name == "id" {
							idFieldIdx = i
							break
						}
					}
					if idFieldIdx != -1 {
						idArr := structArr.Field(idFieldIdx).(*array.Uint16)
						if structArr.IsValid(row) && idArr.IsValid(row) {
							resourceID = strconv.FormatUint(uint64(idArr.Value(row)), 10)
						}
					}
				}
			}

			// Extract span struct id (uint16) for attribute lookup
			idIdx := -1
			for i, f := range spansRec.Schema().Fields() {
				if f.Name == "id" {
					idIdx = i
					break
				}
			}
			if idIdx != -1 {
				col := spansRec.Column(idIdx)
				if arr, ok := col.(*array.Uint16); ok && arr.IsValid(row) {
					spanStructID = strconv.FormatUint(uint64(arr.Value(row)), 10)
				}
			}

			resourceJSON := "{}"
			if res, ok := resourceMap[resourceID]; ok {
				b, _ := json.Marshal(res)
				resourceJSON = string(b)
			}
			attrsJSON := "{}"
			if attrs, ok := spanAttrsMap[spanStructID]; ok {
				b, _ := json.Marshal(attrs)
				attrsJSON = string(b)
			}
			log.Printf("traceID=%q spanID=%q parentID=%q name=%q kind=%q status=%q resourceID=%q resourceJSON=%s attrsJSON=%s", traceID, spanID, parentID, name, kind, status, resourceID, resourceJSON, attrsJSON)
			if err := insertTraceRow(traceID, spanID, parentID, name, kind, status, resourceJSON, attrsJSON); err != nil {
				log.Printf("Error inserting trace row: %v", err)
			}
		}
	}
	// Release Arrow records after processing
	if spansRec != nil { spansRec.Release() }
	if spanAttrsRec != nil { spanAttrsRec.Release() }
	if resourceRec != nil { resourceRec.Release() }
	return nil
}

func ensureTracesTableExists(rec arrow.Record) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS traces (
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

func insertTraceRow(traceID, spanID, parentID, name, kind, status, resourceJSON, attrsJSON string) error {
	_, err := db.Exec(`INSERT INTO traces (trace_id, span_id, parent_id, name, kind, status, resource, attributes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		traceID, spanID, parentID, name, kind, status, resourceJSON, attrsJSON)
	return err
}

// getString decodes a string, dictionary, or binary column (hex for IDs)
func getString(rec arrow.Record, colName string, row int) string {
	idx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == colName {
			idx = i
			break
		}
	}
	log.Printf("getString: colName=%s, idx=%d, numCols=%d", colName, idx, rec.NumCols())
	if idx == -1 || idx >= int(rec.NumCols()) {
		return ""
	}
	col := rec.Column(idx)
	switch arr := col.(type) {
	case *array.String:
		if arr.IsValid(row) {
			return arr.Value(row)
		}
	case *array.Dictionary:
		if arr.IsValid(row) {
			dictArr, ok := arr.Dictionary().(*array.String)
			if ok {
				idx := arr.GetValueIndex(row)
				return dictArr.Value(int(idx))
			}
		}
	case *array.FixedSizeBinary:
		if arr.IsValid(row) {
			return hex.EncodeToString(arr.Value(row))
		}
	case *array.Binary:
		if arr.IsValid(row) {
			return hex.EncodeToString(arr.Value(row))
		}
	}
	return ""
}

// getInt decodes an int32/uint32 column
func getInt(rec arrow.Record, colName string, row int) int64 {
	idx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == colName {
			idx = i
			break
		}
	}
	log.Printf("getInt: colName=%s, idx=%d, numCols=%d", colName, idx, rec.NumCols())
	if idx == -1 || idx >= int(rec.NumCols()) {
		return 0
	}
	col := rec.Column(idx)
	switch arr := col.(type) {
	case *array.Int32:
		if arr.IsValid(row) {
			return int64(arr.Value(row))
		}
	case *array.Uint32:
		if arr.IsValid(row) {
			return int64(arr.Value(row))
		}
	}
	return 0
}

func main() {
	var err error
	db, err = sql.Open("duckdb", "traces.db")
	if err != nil {
		log.Fatalf("failed to open DuckDB: %v", err)
	}

	lis, err := net.Listen("tcp", ":9002")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	arrowpb.RegisterArrowTracesServiceServer(grpcServer, &server{})
	log.Println("ArrowTracesService gRPC server listening on :9002")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
} 