package server

import (
	"net/http"

	"github.com/goccy/go-json"
)

// corsMiddleware adds CORS headers
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip CORS for WebSocket upgrade requests
		if r.Header.Get("Upgrade") == "websocket" {
			next.ServeHTTP(w, r)
			return
		}

		// Normal CORS handling
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")

		if requestedHeaders := r.Header.Get("Access-Control-Request-Headers"); requestedHeaders != "" {
			w.Header().Set("Access-Control-Allow-Headers", requestedHeaders)
		} else {
			w.Header().Set("Access-Control-Allow-Headers", "*")
		}

		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// sendJSON sends a JSON response
func sendJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	jsonData, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(`{"error":"failed to marshal JSON"}`))
		return
	}

	w.WriteHeader(statusCode)
	w.Write(jsonData)
}
