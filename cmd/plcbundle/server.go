package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/gorilla/websocket"
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/plc"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins (adjust for production)
	},
}

func handleRoot(w http.ResponseWriter, r *http.Request, mgr *bundle.Manager, syncMode bool, wsEnabled bool) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	index := mgr.GetIndex()
	stats := index.GetStats()
	bundleCount := stats["bundle_count"].(int)

	baseURL := getBaseURL(r)
	wsURL := getWSURL(r)

	fmt.Fprintf(w, `

	⠀⠀⠀⠀⠀⠀⠀⠀⠀⠄⠀⡀⠀⠀⠀⠀⠀⠀⢀⠀⠀⡀⠀⢀⠀⢀⡀⣤⡢⣤⡤⡀⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⡄⡄⠐⡀⠈⣀⠀⡠⡠⠀⣢⣆⢌⡾⢙⠺⢽⠾⡋⣻⡷⡫⢵⣭⢦⣴⠦⠀⢠⠀⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢠⣤⣽⣥⡈⠧⣂⢧⢾⠕⠞⠡⠊⠁⣐⠉⠀⠉⢍⠀⠉⠌⡉⠀⠂⠁⠱⠉⠁⢝⠻⠎⣬⢌⡌⣬⣡⣀⣢⣄⡄⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢀⢸⣿⣿⢿⣾⣯⣑⢄⡂⠀⠄⠂⠀⠀⢀⠀⠀⠐⠀⠀⠀⠀⠀⠀⠀⠀⠄⠐⠀⠀⠀⠀⣄⠭⠂⠈⠜⣩⣿⢝⠃⠀⠁⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢀⣻⡟⠏⠀⠚⠈⠚⡉⡝⢶⣱⢤⣅⠈⠀⠄⠀⠀⠀⠀⠀⠠⠀⠀⡂⠐⣤⢕⡪⢼⣈⡹⡇⠏⠏⠋⠅⢃⣪⡏⡇⡍⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠺⣻⡄⠀⠀⠀⢠⠌⠃⠐⠉⢡⠱⠧⠝⡯⣮⢶⣴⣤⡆⢐⣣⢅⣮⡟⠦⠍⠉⠀⠁⠐⠀⠀⠀⠄⠐⠡⣽⡸⣎⢁⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢈⡻⣧⠀⠁⠐⠀⠀⠀⠀⠀⠀⠊⠀⠕⢀⡉⠈⡫⠽⡿⡟⠿⠟⠁⠀⠀⠄⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠬⠥⣋⡯⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⡀⣾⡍⠕⡀⠀⠀⠀⠄⠠⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠥⣤⢌⠀⠀⠀⠀⠀⠀⠀⠀⠀⠁⠀⠀⠄⢀⠀⢝⢞⣫⡆⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣽⡶⡄⠐⡀⠀⠀⠀⠀⠀⠀⢀⠀⠄⠀⠀⠀⠄⠁⠇⣷⡆⠀⠀⠀⢀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡸⢝⣮⠍⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⢀⠀⢾⣷⠀⠠⡀⠀⠀⠀⠀⢀⠀⠀⠀⠀⠀⠁⡁⠀⠀⣾⡥⠖⠀⠀⠀⠂⠀⠀⠀⠀⠀⠁⠀⡀⠁⠀⠀⠻⢳⣻⢄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣞⡙⠨⣀⠠⠄⠀⠂⠀⠀⠀⠈⢀⠀⠀⠀⠀⠀⠤⢚⢢⣟⠀⠀⠀⠀⡐⠀⠀⡀⠀⠀⠀⠀⠁⠈⠌⠊⣯⣮⡏⠡⠂⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣻⡟⡄⡡⣄⠀⠠⠀⠀⡅⠀⠐⠀⡀⠀⡀⠀⠄⠈⠃⠳⠪⠤⠀⠀⠀⠀⡀⠀⠂⠀⠀⠀⠁⠈⢠⣠⠒⠻⣻⡧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠪⡎⠠⢌⠑⡀⠂⠀⠄⠠⠀⠠⠀⠁⡀⠠⠠⡀⣀⠜⢏⡅⠀⠀⡀⠁⠀⠀⠁⠁⠐⠄⡀⢀⠂⠀⠄⢑⣿⣿⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠼⣻⠧⣣⣀⠐⠨⠁⠕⢈⢀⢀⡁⠀⠈⠠⢀⠀⠐⠜⣽⡗⡤⠀⠂⠀⠠⠀⢂⠠⠀⠁⠄⠀⠔⠀⠑⣨⣿⢯⠋⡅⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⡚⣷⣭⠎⢃⡗⠄⡄⢀⠁⠀⠅⢀⢅⡀⠠⠀⢠⡀⡩⠷⢇⠀⡀⠄⡠⠤⠆⣀⡀⠄⠉⣠⠃⠴⠀⠈⢁⣿⡛⡯⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠘⡬⡿⣿⡏⡻⡯⠌⢁⢛⠠⠓⠐⠐⠐⠌⠃⠋⠂⡢⢰⣈⢏⣰⠂⠈⠀⠠⠒⠡⠌⠫⠭⠩⠢⡬⠆⠿⢷⢿⡽⡧⠉⠊⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠺⣷⣺⣗⣿⡶⡎⡅⣣⢎⠠⡅⣢⡖⠴⠬⡈⠂⡨⢡⠾⣣⣢⠀⠀⡹⠄⡄⠄⡇⣰⡖⡊⠔⢹⣄⣿⣭⣵⣿⢷⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠩⣿⣿⣲⣿⣷⣟⣼⠟⣬⢉⡠⣪⢜⣂⣁⠥⠓⠚⡁⢶⣷⣠⠂⡄⡢⣀⡐⠧⢆⣒⡲⡳⡫⢟⡃⢪⡧⣟⡟⣯⠐⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢺⠟⢿⢟⢻⡗⡮⡿⣲⢷⣆⣏⣇⡧⣄⢖⠾⡷⣿⣤⢳⢷⣣⣦⡜⠗⣭⢂⠩⣹⢿⡲⢎⡧⣕⣖⣓⣽⡿⡖⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉⠂⠂⠏⠿⢻⣥⡪⢽⣳⣳⣥⡶⣫⣍⢐⣥⣻⣾⡻⣅⢭⡴⢭⣿⠕⣧⡭⣞⣻⣣⣻⢿⠟⠛⠙⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠄⠋⠫⠯⣍⢻⣿⣿⣷⣕⣵⣹⣽⣿⣷⣇⡏⣿⡿⣍⡝⠵⠯⠁⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⠠⠁⠋⢣⠓⡍⣫⠹⣿⣿⣷⡿⠯⠺⠁⠁⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠋⢀⠋⢈⡿⠿⠁⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀

plcbundle %s

*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
| ⚠️ Preview Version – Do Not Use In Production!                 |
*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
| This project and plcbundle specification is currently          |
| unstable and under heavy development. Things can break at      |
| any time. Do not use this for production systems.              |
| Please wait for the 1.0 release.                               |
|________________________________________________________________|

`, version)

	fmt.Fprintf(w, "What is PLC Bundle?\n")
	fmt.Fprintf(w, "━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Fprintf(w, "plcbundle archives AT Protocol's DID PLC Directory operations into\n")
	fmt.Fprintf(w, "immutable, cryptographically-chained bundles of 10,000 operations.\n")
	fmt.Fprintf(w, "Each bundle is hashed (SHA-256), compressed (zstd), and linked to\n")
	fmt.Fprintf(w, "the previous bundle, creating a verifiable chain of DID operations.\n\n")
	fmt.Fprintf(w, "More info: https://tangled.org/@atscan.net/plcbundle\n\n")

	fmt.Fprintf(w, "Server Stats\n")
	fmt.Fprintf(w, "━━━━━━━━━━━━\n")
	fmt.Fprintf(w, "  Bundle count:  %d\n", bundleCount)
	fmt.Fprintf(w, "  Sync mode:     %v\n", syncMode)
	fmt.Fprintf(w, "  WebSocket:     %v\n", wsEnabled)

	if bundleCount > 0 {
		firstBundle := stats["first_bundle"].(int)
		lastBundle := stats["last_bundle"].(int)
		totalSize := stats["total_size"].(int64)

		fmt.Fprintf(w, "  Range:         %06d - %06d\n", firstBundle, lastBundle)
		fmt.Fprintf(w, "  Total size:    %.2f MB\n", float64(totalSize)/(1000*1000))
		fmt.Fprintf(w, "  Updated:       %s\n", stats["updated_at"].(time.Time).Format("2006-01-02 15:04:05"))

		if gaps, ok := stats["gaps"].(int); ok && gaps > 0 {
			fmt.Fprintf(w, "  ⚠ Gaps:        %d missing bundles\n", gaps)
		}

		// Get first and last bundle metadata for hashes
		firstMeta, err := index.GetBundle(firstBundle)
		if err == nil {
			fmt.Fprintf(w, "\n  Root: %s\n", firstMeta.Hash)
		}

		lastMeta, err := index.GetBundle(lastBundle)
		if err == nil {
			fmt.Fprintf(w, "  Head: %s\n", lastMeta.Hash)
		}
	}

	// Show mempool stats if sync mode
	if syncMode {
		mempoolStats := mgr.GetMempoolStats()
		count := mempoolStats["count"].(int)
		targetBundle := mempoolStats["target_bundle"].(int)
		canCreate := mempoolStats["can_create_bundle"].(bool)

		fmt.Fprintf(w, "\nMempool Stats\n")
		fmt.Fprintf(w, "━━━━━━━━━━━━━\n")
		fmt.Fprintf(w, "  Target bundle:     %06d\n", targetBundle)
		fmt.Fprintf(w, "  Operations:        %d / %d\n", count, bundle.BUNDLE_SIZE)
		fmt.Fprintf(w, "  Can create bundle: %v\n", canCreate)

		if count > 0 {
			progress := float64(count) / float64(bundle.BUNDLE_SIZE) * 100
			fmt.Fprintf(w, "  Progress:          %.1f%%\n", progress)

			// ASCII Progress bar
			barWidth := 50
			filled := int(float64(barWidth) * float64(count) / float64(bundle.BUNDLE_SIZE))
			if filled > barWidth {
				filled = barWidth
			}
			bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
			fmt.Fprintf(w, "  [%s]\n", bar)

			if firstTime, ok := mempoolStats["first_time"].(time.Time); ok {
				fmt.Fprintf(w, "  First op:          %s\n", firstTime.Format("2006-01-02 15:04:05"))
			}
			if lastTime, ok := mempoolStats["last_time"].(time.Time); ok {
				fmt.Fprintf(w, "  Last op:           %s\n", lastTime.Format("2006-01-02 15:04:05"))
			}
		} else {
			fmt.Fprintf(w, "  (empty)\n")
		}
	}

	fmt.Fprintf(w, "\nAPI Endpoints\n")
	fmt.Fprintf(w, "━━━━━━━━━━━━━\n")
	fmt.Fprintf(w, "  GET  /                    This info page\n")
	fmt.Fprintf(w, "  GET  /index.json          Full bundle index\n")
	fmt.Fprintf(w, "  GET  /bundle/:number      Bundle metadata (JSON)\n")
	fmt.Fprintf(w, "  GET  /data/:number        Raw bundle (zstd compressed)\n")
	fmt.Fprintf(w, "  GET  /jsonl/:number       Decompressed JSONL stream\n")

	if wsEnabled {
		fmt.Fprintf(w, "\nWebSocket Endpoints\n")
		fmt.Fprintf(w, "━━━━━━━━━━━━━━━━━━━\n")
		fmt.Fprintf(w, "  WS   /ws?cursor=N         Live stream all records from cursor N\n")
		fmt.Fprintf(w, "                            Streams all bundles, then mempool\n")
		fmt.Fprintf(w, "                            Continues streaming new operations live\n")
		fmt.Fprintf(w, "                            Connection stays open until client closes\n")
		fmt.Fprintf(w, "                            Cursor: global record number (0-based)\n")
		fmt.Fprintf(w, "                            Example: 88410345 = bundle 8841, pos 345\n")
	}

	if syncMode {
		fmt.Fprintf(w, "\nSync Endpoints\n")
		fmt.Fprintf(w, "━━━━━━━━━━━━━━\n")
		fmt.Fprintf(w, "  GET  /sync                Sync status & mempool info (JSON)\n")
		fmt.Fprintf(w, "  GET  /sync/mempool        Mempool operations (JSONL)\n")
	}

	fmt.Fprintf(w, "\nExamples\n")
	fmt.Fprintf(w, "━━━━━━━━\n")
	fmt.Fprintf(w, "  # Get bundle metadata\n")
	fmt.Fprintf(w, "  curl %s/bundle/1\n\n", baseURL)
	fmt.Fprintf(w, "  # Download compressed bundle 42\n")
	fmt.Fprintf(w, "  curl %s/data/42 -o 000042.jsonl.zst\n\n", baseURL)
	fmt.Fprintf(w, "  # Stream decompressed operations from bundle 42\n")
	fmt.Fprintf(w, "  curl %s/jsonl/1\n\n", baseURL)

	if wsEnabled {
		fmt.Fprintf(w, "  # Stream all operations via WebSocket (from beginning)\n")
		fmt.Fprintf(w, "  websocat %s/ws\n\n", wsURL)
		fmt.Fprintf(w, "  # Stream from cursor 10000\n")
		fmt.Fprintf(w, "  websocat '%s/ws?cursor=10000'\n\n", wsURL)
		fmt.Fprintf(w, "  # Stream and save to file\n")
		fmt.Fprintf(w, "  websocat %s/ws > all_operations.jsonl\n\n", wsURL)
		fmt.Fprintf(w, "  # Stream with jq for pretty printing\n")
		fmt.Fprintf(w, "  websocat %s/ws | jq .\n\n", wsURL)
	}

	if syncMode {
		fmt.Fprintf(w, "  # Get sync status\n")
		fmt.Fprintf(w, "  curl %s/sync\n\n", baseURL)
		fmt.Fprintf(w, "  # Get mempool operations\n")
		fmt.Fprintf(w, "  curl %s/sync/mempool\n\n", baseURL)
	}

	fmt.Fprintf(w, "\n────────────────────────────────────────────────────────────────\n")
	fmt.Fprintf(w, "plcbundle %s | https://tangled.org/@atscan.net/plcbundle\n", version)
}

// getScheme returns the appropriate HTTP scheme (http or https)
func getScheme(r *http.Request) string {
	// Check if TLS is active
	if r.TLS != nil {
		return "https"
	}

	// Check X-Forwarded-Proto header (set by reverse proxies)
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		return proto
	}

	// Check if behind a proxy with X-Forwarded-Ssl
	if r.Header.Get("X-Forwarded-Ssl") == "on" {
		return "https"
	}

	// Default to http
	return "http"
}

// getWSScheme returns the appropriate WebSocket scheme (ws or wss)
func getWSScheme(r *http.Request) string {
	if getScheme(r) == "https" {
		return "wss"
	}
	return "ws"
}

// getBaseURL returns the full base URL (with scheme)
func getBaseURL(r *http.Request) string {
	scheme := getScheme(r)
	return fmt.Sprintf("%s://%s", scheme, r.Host)
}

// getWSURL returns the WebSocket base URL
func getWSURL(r *http.Request) string {
	scheme := getWSScheme(r)
	return fmt.Sprintf("%s://%s", scheme, r.Host)
}

func newServerHandler(mgr *bundle.Manager, syncMode bool, wsEnabled bool) http.Handler {
	mux := http.NewServeMux()

	// Root - ASCII art + info
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		handleRoot(w, r, mgr, syncMode, wsEnabled)
	})

	// Index JSON (reload from disk each time for fresh data during rebuild)
	mux.HandleFunc("/index.json", func(w http.ResponseWriter, r *http.Request) {
		// Reload index to get latest data
		mgr.GetIndex() // This will refresh if needed
		handleIndexJSON(w, mgr)
	})

	// Bundle metadata
	mux.HandleFunc("/bundle/", func(w http.ResponseWriter, r *http.Request) {
		handleBundle(w, r, mgr)
	})

	// Bundle data (raw compressed)
	mux.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		handleBundleData(w, r, mgr)
	})

	// Bundle JSONL (decompressed)
	mux.HandleFunc("/jsonl/", func(w http.ResponseWriter, r *http.Request) {
		handleBundleJSONL(w, r, mgr)
	})

	// WebSocket endpoint (if enabled)
	if wsEnabled {
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			handleWebSocket(w, r, mgr)
		})
	}

	// Sync endpoints (only if sync mode enabled)
	if syncMode {
		mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
			handleSync(w, mgr)
		})

		mux.HandleFunc("/sync/mempool", func(w http.ResponseWriter, r *http.Request) {
			handleSyncMempool(w, mgr)
		})
	}

	return mux
}

// handleWebSocket streams all records via WebSocket starting from cursor
// Keeps connection alive and streams new records as they arrive
func handleWebSocket(w http.ResponseWriter, r *http.Request, mgr *bundle.Manager) {
	// Parse cursor from query parameter (defaults to 0)
	cursorStr := r.URL.Query().Get("cursor")
	cursor := 0
	if cursorStr != "" {
		var err error
		cursor, err = strconv.Atoi(cursorStr)
		if err != nil || cursor < 0 {
			http.Error(w, "Invalid cursor: must be non-negative integer", http.StatusBadRequest)
			return
		}
	}

	// Upgrade to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WebSocket upgrade failed: %v\n", err)
		return
	}
	defer conn.Close()

	// Set up handlers for connection management
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Channel to signal client disconnect
	done := make(chan struct{})

	// Start goroutine to detect client disconnect
	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					fmt.Fprintf(os.Stderr, "WebSocket: client closed connection\n")
				}
				return
			}
		}
	}()

	ctx := context.Background()

	// Stream all data and keep connection alive
	if err := streamLive(ctx, conn, mgr, cursor, done); err != nil {
		fmt.Fprintf(os.Stderr, "WebSocket stream error: %v\n", err)
	}
}

// streamLive streams all historical data then continues with live updates
func streamLive(ctx context.Context, conn *websocket.Conn, mgr *bundle.Manager, startCursor int, done chan struct{}) error {
	index := mgr.GetIndex()
	bundles := index.GetBundles()
	currentRecord := startCursor

	// Step 1: Stream historical bundles
	if len(bundles) > 0 {
		startBundleIdx := startCursor / bundle.BUNDLE_SIZE
		startPosition := startCursor % bundle.BUNDLE_SIZE

		if startBundleIdx < len(bundles) {
			// Stream from startBundleIdx to end
			for i := startBundleIdx; i < len(bundles); i++ {
				skipUntil := 0
				if i == startBundleIdx {
					skipUntil = startPosition
				}

				newRecordCount, err := streamBundle(ctx, conn, mgr, bundles[i].BundleNumber, skipUntil, done)
				if err != nil {
					return err
				}
				currentRecord += newRecordCount
			}
		}
	}

	// Step 2: Stream current mempool
	lastSeenMempoolCount := 0
	if err := streamMempool(conn, mgr, startCursor, len(bundles)*bundle.BUNDLE_SIZE, &currentRecord, &lastSeenMempoolCount, done); err != nil {
		return err
	}

	// Step 3: Live streaming loop
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	lastBundleCount := len(bundles)
	fmt.Fprintf(os.Stderr, "WebSocket: entering live mode at cursor %d\n", currentRecord)

	for {
		select {
		case <-done:
			fmt.Fprintf(os.Stderr, "WebSocket: client disconnected, stopping stream\n")
			return nil

		case <-ticker.C:
			// Check for new bundles
			index = mgr.GetIndex()
			bundles = index.GetBundles()

			if len(bundles) > lastBundleCount {
				fmt.Fprintf(os.Stderr, "WebSocket: detected %d new bundle(s)\n", len(bundles)-lastBundleCount)

				// Stream new bundles
				for i := lastBundleCount; i < len(bundles); i++ {
					newRecordCount, err := streamBundle(ctx, conn, mgr, bundles[i].BundleNumber, 0, done)
					if err != nil {
						return err
					}
					currentRecord += newRecordCount
				}

				lastBundleCount = len(bundles)
				lastSeenMempoolCount = 0 // Reset after bundle creation
			}

			// Check for new mempool operations
			if err := streamMempool(conn, mgr, startCursor, len(bundles)*bundle.BUNDLE_SIZE, &currentRecord, &lastSeenMempoolCount, done); err != nil {
				return err
			}

			// Keep-alive ping
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return err
			}
		}
	}
}

// streamBundle streams a single bundle's operations without parsing
// Returns number of records streamed
func streamBundle(ctx context.Context, conn *websocket.Conn, mgr *bundle.Manager, bundleNumber int, skipUntil int, done chan struct{}) (int, error) {
	reader, err := mgr.StreamBundleDecompressed(ctx, bundleNumber)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to stream bundle %d: %v\n", bundleNumber, err)
		return 0, nil // Continue with next bundle
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	position := 0
	streamed := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Skip until start position
		if position < skipUntil {
			position++
			continue
		}

		select {
		case <-done:
			return streamed, nil
		default:
		}

		// Send raw JSON line
		if err := conn.WriteMessage(websocket.TextMessage, line); err != nil {
			return streamed, err
		}

		position++
		streamed++

		if streamed%1000 == 0 {
			conn.WriteMessage(websocket.PingMessage, nil)
		}
	}

	if err := scanner.Err(); err != nil {
		return streamed, fmt.Errorf("scanner error on bundle %d: %w", bundleNumber, err)
	}

	return streamed, nil
}

// streamMempool streams new operations from mempool
func streamMempool(conn *websocket.Conn, mgr *bundle.Manager, startCursor int, bundleRecordBase int, currentRecord *int, lastSeenCount *int, done chan struct{}) error {
	mempoolOps, err := mgr.GetMempoolOperations()
	if err != nil {
		return nil // Not fatal
	}

	if len(mempoolOps) <= *lastSeenCount {
		return nil // No new operations
	}

	newOps := len(mempoolOps) - *lastSeenCount
	if newOps > 0 {
		fmt.Fprintf(os.Stderr, "WebSocket: streaming %d new mempool operation(s)\n", newOps)
	}

	for i := *lastSeenCount; i < len(mempoolOps); i++ {
		recordNum := bundleRecordBase + i
		if recordNum < startCursor {
			continue
		}

		select {
		case <-done:
			return nil
		default:
		}

		if err := sendOperation(conn, mempoolOps[i]); err != nil {
			return err
		}
		*currentRecord++
	}

	*lastSeenCount = len(mempoolOps)
	return nil
}

// sendOperation sends a single operation over WebSocket as raw JSON
func sendOperation(conn *websocket.Conn, op plc.PLCOperation) error {
	var data []byte
	var err error

	// Use raw JSON if available, otherwise marshal
	if len(op.RawJSON) > 0 {
		data = op.RawJSON
	} else {
		data, err = json.Marshal(op)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to marshal operation: %v\n", err)
			return nil // Skip this operation but continue
		}
	}

	// Send as text message
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			fmt.Fprintf(os.Stderr, "WebSocket write error: %v\n", err)
		}
		return err
	}

	return nil
}

// handleSync returns sync status and mempool info as JSON
func handleSync(w http.ResponseWriter, mgr *bundle.Manager) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	index := mgr.GetIndex()
	indexStats := index.GetStats()
	mempoolStats := mgr.GetMempoolStats()

	// Build response
	response := map[string]interface{}{
		"bundles": map[string]interface{}{
			"count":      indexStats["bundle_count"],
			"total_size": indexStats["total_size"],
			"updated_at": indexStats["updated_at"],
		},
		"mempool": mempoolStats,
	}

	// Add bundle range if bundles exist
	if bundleCount, ok := indexStats["bundle_count"].(int); ok && bundleCount > 0 {
		response["bundles"].(map[string]interface{})["first_bundle"] = indexStats["first_bundle"]
		response["bundles"].(map[string]interface{})["last_bundle"] = indexStats["last_bundle"]
		response["bundles"].(map[string]interface{})["start_time"] = indexStats["start_time"]
		response["bundles"].(map[string]interface{})["end_time"] = indexStats["end_time"]

		if gaps, ok := indexStats["gaps"].(int); ok {
			response["bundles"].(map[string]interface{})["gaps"] = gaps
		}
	}

	// Calculate mempool progress percentage
	if count, ok := mempoolStats["count"].(int); ok {
		progress := float64(count) / float64(bundle.BUNDLE_SIZE) * 100
		response["mempool"].(map[string]interface{})["progress_percent"] = progress
		response["mempool"].(map[string]interface{})["bundle_size"] = bundle.BUNDLE_SIZE
	}

	data, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		http.Error(w, "Failed to marshal sync status", http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

// handleSyncMempool streams mempool operations as JSONL
func handleSyncMempool(w http.ResponseWriter, mgr *bundle.Manager) {
	ops, err := mgr.GetMempoolOperations()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get mempool operations: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if len(ops) == 0 {
		// Return empty response
		return
	}

	// Stream operations as JSONL
	for _, op := range ops {
		if len(op.RawJSON) > 0 {
			w.Write(op.RawJSON)
		} else {
			// Fallback to marshaling if no raw JSON
			data, _ := json.Marshal(op)
			w.Write(data)
		}
		w.Write([]byte("\n"))
	}
}

func handleIndexJSON(w http.ResponseWriter, mgr *bundle.Manager) {
	index := mgr.GetIndex()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		http.Error(w, "Failed to marshal index", http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func handleBundle(w http.ResponseWriter, r *http.Request, mgr *bundle.Manager) {
	// Extract bundle number from URL
	path := strings.TrimPrefix(r.URL.Path, "/bundle/")

	var bundleNum int
	if _, err := fmt.Sscanf(path, "%d", &bundleNum); err != nil {
		http.Error(w, "Invalid bundle number", http.StatusBadRequest)
		return
	}

	meta, err := mgr.GetIndex().GetBundle(bundleNum)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		http.Error(w, "Failed to marshal metadata", http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func handleBundleData(w http.ResponseWriter, r *http.Request, mgr *bundle.Manager) {
	// Extract bundle number from URL
	path := strings.TrimPrefix(r.URL.Path, "/data/")

	var bundleNum int
	if _, err := fmt.Sscanf(path, "%d", &bundleNum); err != nil {
		http.Error(w, "Invalid bundle number", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	reader, err := mgr.StreamBundleRaw(ctx, bundleNum)
	if err != nil {
		if strings.Contains(err.Error(), "not in index") || strings.Contains(err.Error(), "not found") {
			http.NotFound(w, r)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer reader.Close()

	// Set headers
	w.Header().Set("Content-Type", "application/zstd")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%06d.jsonl.zst", bundleNum))
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Stream the data
	if _, err := io.Copy(w, reader); err != nil {
		// Can't send error after streaming started
		fmt.Fprintf(os.Stderr, "Error streaming bundle %d: %v\n", bundleNum, err)
	}
}

func handleBundleJSONL(w http.ResponseWriter, r *http.Request, mgr *bundle.Manager) {
	// Extract bundle number from URL
	path := strings.TrimPrefix(r.URL.Path, "/jsonl/")

	var bundleNum int
	if _, err := fmt.Sscanf(path, "%d", &bundleNum); err != nil {
		http.Error(w, "Invalid bundle number", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	reader, err := mgr.StreamBundleDecompressed(ctx, bundleNum)
	if err != nil {
		if strings.Contains(err.Error(), "not in index") || strings.Contains(err.Error(), "not found") {
			http.NotFound(w, r)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer reader.Close()

	// Set headers
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%06d.jsonl", bundleNum))
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Stream the data
	if _, err := io.Copy(w, reader); err != nil {
		// Can't send error after streaming started
		fmt.Fprintf(os.Stderr, "Error streaming bundle %d: %v\n", bundleNum, err)
	}
}

// runSync continuously fetches new bundles in the background
func runSync(ctx context.Context, mgr *bundle.Manager, interval time.Duration) {
	fmt.Printf("[Sync] Starting sync loop (interval: %s)\n", interval)

	// Do initial sync immediately
	syncBundles(ctx, mgr)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Periodic mempool save (every 5 minutes)
	saveTicker := time.NewTicker(5 * time.Minute)
	defer saveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Save on shutdown
			if err := mgr.SaveMempool(); err != nil {
				fmt.Printf("[Sync] Failed to save mempool on shutdown: %v\n", err)
			}
			fmt.Printf("[Sync] Sync stopped\n")
			return

		case <-ticker.C:
			syncBundles(ctx, mgr)

		case <-saveTicker.C:
			// Periodic save (every 5 min) - only if mempool has data
			stats := mgr.GetMempoolStats()
			if stats["count"].(int) > 0 {
				fmt.Printf("[Sync] Saving mempool (%d ops)...\n", stats["count"])
				if err := mgr.SaveMempool(); err != nil {
					fmt.Printf("[Sync] Failed to save mempool: %v\n", err)
				}
			}
		}
	}
}

// syncBundles fetches all available bundles
func syncBundles(ctx context.Context, mgr *bundle.Manager) {
	index := mgr.GetIndex()
	lastBundle := index.GetLastBundle()
	startBundle := 1
	if lastBundle != nil {
		startBundle = lastBundle.BundleNumber + 1
	}

	fmt.Printf("[Sync] Checking for new bundles (current: %06d)...\n", startBundle-1)

	fetchedCount := 0
	consecutiveErrors := 0
	maxConsecutiveErrors := 3

	for {
		currentBundle := startBundle + fetchedCount

		b, err := mgr.FetchNextBundle(ctx)
		if err != nil {
			// Check if we've reached the end
			if isEndOfDataError(err) {
				if fetchedCount > 0 {
					fmt.Printf("[Sync] ✓ Synced %d new bundles (now at %06d)\n",
						fetchedCount, currentBundle-1)
				} else {
					fmt.Printf("[Sync] ✓ Already up to date (bundle %06d)\n", startBundle-1)
				}
				break
			}

			// Handle other errors
			consecutiveErrors++
			fmt.Fprintf(os.Stderr, "[Sync] Error fetching bundle %06d: %v\n", currentBundle, err)

			if consecutiveErrors >= maxConsecutiveErrors {
				fmt.Fprintf(os.Stderr, "[Sync] Too many consecutive errors, stopping sync\n")
				break
			}

			// Wait before retry
			time.Sleep(5 * time.Second)
			continue
		}

		// Reset error counter on success
		consecutiveErrors = 0

		if err := mgr.SaveBundle(ctx, b); err != nil {
			fmt.Fprintf(os.Stderr, "[Sync] Error saving bundle %06d: %v\n", b.BundleNumber, err)
			break
		}

		fetchedCount++
		fmt.Printf("[Sync] ✓ Fetched bundle %06d (%d ops, %d DIDs)\n",
			b.BundleNumber, len(b.Operations), b.DIDCount)

		// Add a small delay between fetches to be nice to the PLC directory
		time.Sleep(500 * time.Millisecond)
	}
}
