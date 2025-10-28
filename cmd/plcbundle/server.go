package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/atscan/plcbundle/bundle"
)

func newServerHandler(mgr *bundle.Manager, syncMode bool) http.Handler {
	mux := http.NewServeMux()

	// Root - ASCII art + info
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		handleRoot(w, r, mgr, syncMode)
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

func handleRoot(w http.ResponseWriter, r *http.Request, mgr *bundle.Manager, syncMode bool) {
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
	fmt.Fprintf(w, "  Sync mode:     %v\n", syncMode)

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

	if syncMode {
		fmt.Fprintf(w, "\nSync Endpoints\n")
		fmt.Fprintf(w, "━━━━━━━━━━━━━━\n")
		fmt.Fprintf(w, "  GET  /sync                Sync status & mempool info (JSON)\n")
		fmt.Fprintf(w, "  GET  /sync/mempool        Mempool operations (JSONL)\n")
	}

	fmt.Fprintf(w, "\nExamples\n")
	fmt.Fprintf(w, "━━━━━━━━\n")
	fmt.Fprintf(w, "  # Get bundle metadata\n")
	fmt.Fprintf(w, "  curl http://%s/bundle/1\n\n", r.Host)
	fmt.Fprintf(w, "  # Download compressed bundle\n")
	fmt.Fprintf(w, "  curl http://%s/data/1 -o 000001.jsonl.zst\n\n", r.Host)
	fmt.Fprintf(w, "  # Stream decompressed operations\n")
	fmt.Fprintf(w, "  curl http://%s/jsonl/1\n\n", r.Host)

	if syncMode {
		fmt.Fprintf(w, "  # Get sync status\n")
		fmt.Fprintf(w, "  curl http://%s/sync\n\n", r.Host)
		fmt.Fprintf(w, "  # Get mempool operations\n")
		fmt.Fprintf(w, "  curl http://%s/sync/mempool\n\n", r.Host)
	}

	fmt.Fprintf(w, "\n────────────────────────────────────────────────────────────────\n")
	fmt.Fprintf(w, "plcbundle v%s | https://github.com/atscan/plcbundle\n", version)
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

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[Sync] Sync stopped\n")
			return
		case <-ticker.C:
			syncBundles(ctx, mgr)
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
