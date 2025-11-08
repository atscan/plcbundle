package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	"tangled.org/atscan.net/plcbundle/internal/didindex"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/server"
)

// ServerCommand handles the serve subcommand
func ServerCommand(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	port := fs.String("port", "8080", "HTTP server port")
	host := fs.String("host", "127.0.0.1", "HTTP server host")
	syncMode := fs.Bool("sync", false, "enable sync mode (auto-sync from PLC)")
	plcURL := fs.String("plc", "https://plc.directory", "PLC directory URL (for sync mode)")
	syncInterval := fs.Duration("sync-interval", 1*time.Minute, "sync interval for sync mode")
	enableWebSocket := fs.Bool("websocket", false, "enable WebSocket endpoint for streaming")
	enableResolver := fs.Bool("resolver", false, "enable DID resolution endpoints")
	workers := fs.Int("workers", 0, "number of workers for auto-rebuild (0 = CPU count)")
	verbose := fs.Bool("verbose", false, "verbose sync logging")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Auto-detect CPU count
	if *workers == 0 {
		*workers = runtime.NumCPU()
	}

	// Get working directory
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get working directory: %w", err)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create manager config
	config := bundle.DefaultConfig(dir)
	config.RebuildWorkers = *workers
	config.RebuildProgress = func(current, total int) {
		if current%100 == 0 || current == total {
			fmt.Printf("  Rebuild progress: %d/%d bundles (%.1f%%)    \r",
				current, total, float64(current)/float64(total)*100)
			if current == total {
				fmt.Println()
			}
		}
	}

	// Create PLC client if sync mode enabled
	var client *plcclient.Client
	if *syncMode {
		client = plcclient.NewClient(*plcURL)
	}

	fmt.Printf("Starting plcbundle HTTP server...\n")
	fmt.Printf("  Directory: %s\n", dir)

	// Create manager
	mgr, err := bundle.NewManager(config, client)
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}
	defer mgr.Close()

	// Build/verify DID index if resolver enabled
	if *enableResolver {
		if err := ensureDIDIndex(mgr, *verbose); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️  DID index warning: %v\n\n", err)
		}
	}

	addr := fmt.Sprintf("%s:%s", *host, *port)

	// Display server info
	displayServerInfo(mgr, addr, *syncMode, *enableWebSocket, *enableResolver, *plcURL, *syncInterval)

	// Setup graceful shutdown
	ctx, cancel := setupGracefulShutdown(mgr)
	defer cancel()

	// Start sync loop if enabled
	if *syncMode {
		go runSyncLoop(ctx, mgr, *syncInterval, *verbose, *enableResolver)
	}

	// Create and start HTTP server
	serverConfig := &server.Config{
		Addr:            addr,
		SyncMode:        *syncMode,
		SyncInterval:    *syncInterval,
		EnableWebSocket: *enableWebSocket,
		EnableResolver:  *enableResolver,
		Version:         GetVersion(), // Pass version
	}

	srv := server.New(mgr, serverConfig)

	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

// ensureDIDIndex builds DID index if needed
func ensureDIDIndex(mgr *bundle.Manager, verbose bool) error {
	index := mgr.GetIndex()
	bundleCount := index.Count()
	didStats := mgr.GetDIDIndexStats()

	if bundleCount == 0 {
		return nil
	}

	needsBuild := false
	reason := ""

	if !didStats["exists"].(bool) {
		needsBuild = true
		reason = "index does not exist"
	} else {
		// Check version
		didIndex := mgr.GetDIDIndex()
		if didIndex != nil {
			config := didIndex.GetConfig()
			if config.Version != didindex.DIDINDEX_VERSION {
				needsBuild = true
				reason = fmt.Sprintf("index version outdated (v%d, need v%d)",
					config.Version, didindex.DIDINDEX_VERSION)
			} else {
				// Check if index is behind bundles
				lastBundle := index.GetLastBundle()
				if lastBundle != nil && config.LastBundle < lastBundle.BundleNumber {
					needsBuild = true
					reason = fmt.Sprintf("index is behind (bundle %d, need %d)",
						config.LastBundle, lastBundle.BundleNumber)
				}
			}
		}
	}

	if needsBuild {
		fmt.Printf("  DID Index: BUILDING (%s)\n", reason)
		fmt.Printf("             This may take several minutes...\n\n")

		buildStart := time.Now()
		ctx := context.Background()

		progress := ui.NewProgressBar(bundleCount)
		err := mgr.BuildDIDIndex(ctx, func(current, total int) {
			progress.Set(current)
		})
		progress.Finish()

		if err != nil {
			return fmt.Errorf("failed to build DID index: %w", err)
		}

		buildTime := time.Since(buildStart)
		updatedStats := mgr.GetDIDIndexStats()
		fmt.Printf("\n✓ DID index built in %s\n", buildTime.Round(time.Millisecond))
		fmt.Printf("  Total DIDs: %s\n\n", formatNumber(int(updatedStats["total_dids"].(int64))))
	} else {
		fmt.Printf("  DID Index: ready (%s DIDs)\n",
			formatNumber(int(didStats["total_dids"].(int64))))
	}

	// Verify index consistency
	if didStats["exists"].(bool) {
		fmt.Printf("  Verifying index consistency...\n")

		ctx := context.Background()
		if err := mgr.GetDIDIndex().VerifyAndRepairIndex(ctx, mgr); err != nil {
			return fmt.Errorf("index verification/repair failed: %w", err)
		}
		fmt.Printf("  ✓ Index verified\n")
	}

	return nil
}

// setupGracefulShutdown sets up signal handling for graceful shutdown
func setupGracefulShutdown(mgr *bundle.Manager) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Fprintf(os.Stderr, "\n\n⚠️  Shutdown signal received...\n")
		fmt.Fprintf(os.Stderr, "  Saving mempool...\n")

		if err := mgr.SaveMempool(); err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ Failed to save mempool: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "  ✓ Mempool saved\n")
		}

		fmt.Fprintf(os.Stderr, "  Closing DID index...\n")
		if err := mgr.GetDIDIndex().Close(); err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ Failed to close index: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "  ✓ Index closed\n")
		}

		fmt.Fprintf(os.Stderr, "  ✓ Shutdown complete\n")

		cancel()
		os.Exit(0)
	}()

	return ctx, cancel
}

// displayServerInfo shows server configuration
func displayServerInfo(mgr *bundle.Manager, addr string, syncMode, wsEnabled, resolverEnabled bool, plcURL string, syncInterval time.Duration) {
	fmt.Printf("  Listening: http://%s\n", addr)

	if syncMode {
		fmt.Printf("  Sync mode: ENABLED\n")
		fmt.Printf("  PLC URL: %s\n", plcURL)
		fmt.Printf("  Sync interval: %s\n", syncInterval)
	} else {
		fmt.Printf("  Sync mode: disabled\n")
	}

	if wsEnabled {
		wsScheme := "ws"
		fmt.Printf("  WebSocket: ENABLED (%s://%s/ws)\n", wsScheme, addr)
	} else {
		fmt.Printf("  WebSocket: disabled (use --websocket to enable)\n")
	}

	if resolverEnabled {
		fmt.Printf("  Resolver: ENABLED (/<did> endpoints)\n")
	} else {
		fmt.Printf("  Resolver: disabled (use --resolver to enable)\n")
	}

	bundleCount := mgr.GetIndex().Count()
	if bundleCount > 0 {
		fmt.Printf("  Bundles available: %d\n", bundleCount)
	} else {
		fmt.Printf("  Bundles available: 0\n")
	}

	fmt.Printf("\nPress Ctrl+C to stop\n\n")
}

// runSyncLoop runs the background sync loop
func runSyncLoop(ctx context.Context, mgr *bundle.Manager, interval time.Duration, verbose bool, resolverEnabled bool) {
	// Initial sync
	syncBundles(ctx, mgr, verbose, resolverEnabled)

	fmt.Fprintf(os.Stderr, "[Sync] Starting sync loop (interval: %s)\n", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	saveTicker := time.NewTicker(5 * time.Minute)
	defer saveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := mgr.SaveMempool(); err != nil {
				fmt.Fprintf(os.Stderr, "[Sync] Failed to save mempool: %v\n", err)
			}
			fmt.Fprintf(os.Stderr, "[Sync] Stopped\n")
			return

		case <-ticker.C:
			syncBundles(ctx, mgr, verbose, resolverEnabled)

		case <-saveTicker.C:
			stats := mgr.GetMempoolStats()
			if stats["count"].(int) > 0 && verbose {
				fmt.Fprintf(os.Stderr, "[Sync] Saving mempool (%d ops)\n", stats["count"])
				mgr.SaveMempool()
			}
		}
	}
}

// syncBundles performs a sync cycle
func syncBundles(ctx context.Context, mgr *bundle.Manager, verbose bool, resolverEnabled bool) {
	cycleStart := time.Now()

	index := mgr.GetIndex()
	lastBundle := index.GetLastBundle()
	startBundle := 1
	if lastBundle != nil {
		startBundle = lastBundle.BundleNumber + 1
	}

	mempoolBefore := mgr.GetMempoolStats()["count"].(int)
	fetchedCount := 0

	// ✨ SIMPLIFIED: Just keep calling FetchNextBundle until it fails
	for {
		b, err := mgr.FetchNextBundle(ctx, !verbose)
		if err != nil {
			if isEndOfDataError(err) {
				mempoolAfter := mgr.GetMempoolStats()["count"].(int)
				addedOps := mempoolAfter - mempoolBefore
				duration := time.Since(cycleStart)

				if fetchedCount > 0 {
					fmt.Fprintf(os.Stderr, "[Sync] ✓ Bundle %06d | Synced: %d | Mempool: %d (+%d) | %dms\n",
						startBundle+fetchedCount-1, fetchedCount, mempoolAfter, addedOps, duration.Milliseconds())
				} else {
					fmt.Fprintf(os.Stderr, "[Sync] ✓ Bundle %06d | Up to date | Mempool: %d (+%d) | %dms\n",
						startBundle-1, mempoolAfter, addedOps, duration.Milliseconds())
				}
				break
			}

			// Real error
			fmt.Fprintf(os.Stderr, "[Sync] Error: %v\n", err)
			break
		}

		// Save bundle
		if err := mgr.SaveBundle(ctx, b, !verbose); err != nil {
			fmt.Fprintf(os.Stderr, "[Sync] Error saving bundle %06d: %v\n", b.BundleNumber, err)
			break
		}

		fetchedCount++

		if !verbose {
			fmt.Fprintf(os.Stderr, "[Sync] ✓ %06d | %d ops, %d DIDs\n",
				b.BundleNumber, len(b.Operations), b.DIDCount)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

// isEndOfDataError checks if error indicates end of available data
func isEndOfDataError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return containsAny(errMsg,
		"insufficient operations",
		"no more operations available",
		"reached latest data")
}

// Helper functions

func containsAny(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if contains(s, substr) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
