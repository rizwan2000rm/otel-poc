package internal

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"database/sql"

	log "github.com/sirupsen/logrus"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	arrowpb "tonbo/arrow_receiver/gen"
)

func ProcessTracesBatch(ctx context.Context, db *sql.DB, batch *arrowpb.BatchArrowRecords) error {
	pool := memory.NewGoAllocator()
	var (
		spansRec, spanAttrsRec, resourceRec arrow.Record
		spanAttrsMap = make(map[string]map[string]string)
		resourceMap = make(map[string]map[string]string)
	)
	// 1. Parse all ArrowPayloads and group by type
	for _, payload := range batch.ArrowPayloads {
		rdr, err := ipc.NewReader(bytes.NewReader(payload.Record), ipc.WithAllocator(pool))
		if err != nil {
			log.WithError(err).Error("Error decoding Arrow payload")
			continue
		}
		for rdr.Next() {
			rec := rdr.Record()
			rec.Retain()
			switch payload.Type {
			case 40:
				if spansRec != nil { spansRec.Release() }
				spansRec = rec
			case 41:
				if spanAttrsRec != nil { spanAttrsRec.Release() }
				spanAttrsRec = rec
			case 1:
				if resourceRec != nil { resourceRec.Release() }
				resourceRec = rec
			}
		}
		rdr.Release()
	}
	getFieldNames := func(schema *arrow.Schema) []string {
		names := make([]string, len(schema.Fields()))
		for i, f := range schema.Fields() {
			names[i] = f.Name
		}
		return names
	}
	if spansRec != nil {
		log.WithFields(log.Fields{"schema": getFieldNames(spansRec.Schema()), "num_rows": spansRec.NumRows(), "columns": spansRec.Schema().Fields()}).Debug("SPANS schema")
	}
	if resourceRec != nil {
		log.WithFields(log.Fields{"schema": getFieldNames(resourceRec.Schema()), "num_rows": resourceRec.NumRows(), "columns": resourceRec.Schema().Fields()}).Debug("RESOURCE_ATTRS schema")
	}
	if spanAttrsRec != nil {
		log.WithFields(log.Fields{"schema": getFieldNames(spanAttrsRec.Schema()), "num_rows": spanAttrsRec.NumRows(), "columns": spanAttrsRec.Schema().Fields()}).Debug("SPAN_ATTRS schema")
	}
	if resourceRec != nil {
		for row := 0; row < int(resourceRec.NumRows()); row++ {
			id := ""
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
	if spansRec != nil {
		if err := EnsureTracesTableExists(ctx, db); err != nil {
			return err
		}
		for row := 0; row < int(spansRec.NumRows()); row++ {
			for colIdx, field := range spansRec.Schema().Fields() {
				col := spansRec.Column(colIdx)
				log.WithFields(log.Fields{"row": row, "col": field.Name, "type": col}).Debug("Row column")
			}
			traceID := getString(spansRec, "trace_id", row)
			spanID := getString(spansRec, "span_id", row)
			parentID := getString(spansRec, "parent_span_id", row)
			name := getString(spansRec, "name", row)
			kind := strconv.FormatInt(getInt(spansRec, "kind", row), 10)
			status := strconv.FormatInt(getInt(spansRec, "status", row), 10)
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
			log.WithFields(log.Fields{"traceID": traceID, "spanID": spanID, "parentID": parentID, "name": name, "kind": kind, "status": status, "resourceID": resourceID, "resourceJSON": resourceJSON, "attrsJSON": attrsJSON}).Info("trace row")
			if err := InsertTraceRow(ctx, db, traceID, spanID, parentID, name, kind, status, resourceJSON, attrsJSON); err != nil {
				log.WithError(err).Error("Error inserting trace row")
			}
		}
	}
	if spansRec != nil { spansRec.Release() }
	if spanAttrsRec != nil { spanAttrsRec.Release() }
	if resourceRec != nil { resourceRec.Release() }
	return nil
}

func getString(rec arrow.Record, colName string, row int) string {
	idx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == colName {
			idx = i
			break
		}
	}
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

func getInt(rec arrow.Record, colName string, row int) int64 {
	idx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == colName {
			idx = i
			break
		}
	}
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