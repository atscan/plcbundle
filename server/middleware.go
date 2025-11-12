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

		// Set CORS headers for all requests
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Max-Age", "86400")
		w.Header().Set("Access-Control-Expose-Headers",
			"X-Bundle-Number, X-Position, X-Global-Position, X-Pointer, "+
				"X-Operation-DID, X-Operation-CID, X-Load-Time-Ms, X-Total-Time-Ms, "+
				"X-Resolution-Time-Ms, X-Resolution-Source, X-Index-Time-Ms")

		// Handle OPTIONS preflight - return immediately WITHOUT calling handler
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent) // 204
			return                              // STOP HERE - don't call next
		}

		// Only call handler for non-OPTIONS requests
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
