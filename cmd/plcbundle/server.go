package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/atscan/plcbundle/bundle"
)

func newServerHandler(mgr *bundle.Manager, dir string) http.Handler {
	mux := http.NewServeMux()

	// Root - ASCII art + info
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		handleRoot(w, r, mgr)
	})

	// Index JSON
	mux.HandleFunc("/index.json", func(w http.ResponseWriter, r *http.Request) {
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

	return mux
}

func handleRoot(w http.ResponseWriter, r *http.Request, mgr *bundle.Manager) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	index := mgr.GetIndex()
	stats := index.GetStats()
	bundleCount := stats["bundle_count"].(int)

	fmt.Fprint(w, `
||||| PLC Bundle Server |||||

`)

	fmt.Fprintf(w, "What is PLC Bundle?\n")
	fmt.Fprintf(w, "━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Fprintf(w, "plcbundle archives AT Protocol's PLC directory operations into\n")
	fmt.Fprintf(w, "immutable, cryptographically-chained bundles of 10,000 operations.\n")
	fmt.Fprintf(w, "Each bundle is compressed (zstd), hashed (SHA-256), and linked to\n")
	fmt.Fprintf(w, "the previous bundle, creating a verifiable chain of DID operations.\n\n")
	fmt.Fprintf(w, "More info: https://github.com/atscan/plcbundle\n\n")

	fmt.Fprintf(w, "Server Stats\n")
	fmt.Fprintf(w, "━━━━━━━━━━━━\n")
	fmt.Fprintf(w, "  Bundle count:  %d\n", bundleCount)

	if bundleCount > 0 {
		firstBundle := stats["first_bundle"].(int)
		lastBundle := stats["last_bundle"].(int)
		totalSize := stats["total_size"].(int64)

		fmt.Fprintf(w, "  Range:         %06d - %06d\n", firstBundle, lastBundle)
		fmt.Fprintf(w, "  Total size:    %.2f MB\n", float64(totalSize)/(1024*1024))
		fmt.Fprintf(w, "  Updated:       %s\n", stats["updated_at"].(time.Time).Format("2006-01-02 15:04:05"))

		if gaps, ok := stats["gaps"].(int); ok && gaps > 0 {
			fmt.Fprintf(w, "  ⚠ Gaps:        %d missing bundles\n", gaps)
		}

		// Get first and last bundle metadata for hashes
		firstMeta, err := index.GetBundle(firstBundle)
		if err == nil {
			fmt.Fprintf(w, "\n  Chain root:    %s\n", firstMeta.Hash)
		}

		lastMeta, err := index.GetBundle(lastBundle)
		if err == nil {
			fmt.Fprintf(w, "  Chain head:    %s\n", lastMeta.Hash)
		}
	}

	fmt.Fprintf(w, "\nAPI Endpoints\n")
	fmt.Fprintf(w, "━━━━━━━━━━━━━\n")
	fmt.Fprintf(w, "  GET  /                    This info page\n")
	fmt.Fprintf(w, "  GET  /index.json          Full bundle index\n")
	fmt.Fprintf(w, "  GET  /bundle/:number      Bundle metadata (JSON)\n")
	fmt.Fprintf(w, "  GET  /data/:number        Raw bundle (zstd compressed)\n")
	fmt.Fprintf(w, "  GET  /jsonl/:number       Decompressed JSONL stream\n")

	fmt.Fprintf(w, "\nExamples\n")
	fmt.Fprintf(w, "━━━━━━━━\n")
	fmt.Fprintf(w, "  # Get bundle metadata\n")
	fmt.Fprintf(w, "  curl http://%s/bundle/1\n\n", r.Host)
	fmt.Fprintf(w, "  # Download compressed bundle\n")
	fmt.Fprintf(w, "  curl http://%s/data/1 -o 000001.jsonl.zst\n\n", r.Host)
	fmt.Fprintf(w, "  # Stream decompressed operations\n")
	fmt.Fprintf(w, "  curl http://%s/jsonl/1\n\n", r.Host)
	fmt.Fprintf(w, "  # Get full index\n")
	fmt.Fprintf(w, "  curl http://%s/index.json\n\n", r.Host)

	fmt.Fprintf(w, "\n────────────────────────────────────────────────────────────────\n")
	fmt.Fprintf(w, "plcbundle v%s | https://github.com/atscan/plcbundle\n", version)
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
