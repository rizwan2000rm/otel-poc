package internal

import (
	"database/sql"
	"encoding/json"
	"net/http"

	log "github.com/sirupsen/logrus"
)

// StartQueryAPIServer starts the HTTP server for DuckDB queries
func StartQueryAPIServer(db *sql.DB) {
	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte(`{"error": "POST only"}`))
			return
		}
		var req struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error": "invalid json"}`))
			return
		}
		rows, err := db.Query(req.Query)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			log.WithError(err).Error("query failed")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		results := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"error": "scan failed"}`))
				log.WithError(err).Error("scan failed")
				return
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = vals[i]
			}
			results = append(results, rowMap)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"columns": cols, "rows": results})
	})
	log.Info("HTTP query server listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.WithError(err).Fatal("HTTP server failed")
	}
} 