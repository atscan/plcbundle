package server

import (
	"context"
	"net/http"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/bundle"
)

// Server serves bundle data over HTTP
type Server struct {
	manager    *bundle.Manager
	addr       string
	config     *Config
	startTime  time.Time
	httpServer *http.Server
}

// Config configures the server
type Config struct {
	Addr            string
	SyncMode        bool
	SyncInterval    time.Duration
	EnableWebSocket bool
	EnableResolver  bool
	Version         string
}

// New creates a new HTTP server
func New(manager *bundle.Manager, config *Config) *Server {
	if config.Version == "" {
		config.Version = "dev"
	}

	s := &Server{
		manager:   manager,
		addr:      config.Addr,
		config:    config,
		startTime: time.Now(),
	}

	handler := s.createHandler()

	s.httpServer = &http.Server{
		Addr:    config.Addr,
		Handler: handler,
	}

	return s
}

// ListenAndServe starts the HTTP server
func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// createHandler creates the HTTP handler with all routes
func (s *Server) createHandler() http.Handler {
	mux := http.NewServeMux()

	// Specific routes first
	mux.HandleFunc("GET /index.json", s.handleIndexJSON())
	mux.HandleFunc("GET /bundle/{number}", s.handleBundle())
	mux.HandleFunc("GET /data/{number}", s.handleBundleData())
	mux.HandleFunc("GET /jsonl/{number}", s.handleBundleJSONL())
	mux.HandleFunc("GET /status", s.handleStatus())
	mux.HandleFunc("GET /debug/memory", s.handleDebugMemory())

	// WebSocket
	if s.config.EnableWebSocket {
		mux.HandleFunc("GET /ws", s.handleWebSocket())
	}

	// Sync mode endpoints
	if s.config.SyncMode {
		mux.HandleFunc("GET /mempool", s.handleMempool())
	}

	// Root and DID resolver
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		if path == "/" {
			s.handleRoot()(w, r)
			return
		}

		if s.config.EnableResolver {
			s.handleDIDRouting(w, r)
			return
		}

		sendJSON(w, 404, map[string]string{"error": "not found"})
	})

	return corsMiddleware(mux)
}

// GetStartTime returns when the server started
func (s *Server) GetStartTime() time.Time {
	return s.startTime
}
