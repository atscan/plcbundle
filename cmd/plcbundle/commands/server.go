package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/bundle"
	internalsync "tangled.org/atscan.net/plcbundle/internal/sync"
	"tangled.org/atscan.net/plcbundle/server"
)

func NewServerCommand() *cobra.Command {
	var (
		port            string
		host            string
		syncMode        bool
		plcURL          string
		syncInterval    time.Duration
		enableWebSocket bool
		enableResolver  bool
		maxBundles      int
	)

	cmd := &cobra.Command{
		Use:     "server",
		Aliases: []string{"serve"},
		Short:   "Start HTTP server",
		Long: `Start HTTP server to serve bundles over HTTP

Serves bundle data over HTTP with optional live sync mode that continuously
fetches new bundles from PLC directory.

The server provides:
  - Bundle index (JSON)
  - Individual bundle data (compressed or JSONL)
  - WebSocket streaming (optional)
  - DID resolution (optional)
  - Live mempool (in sync mode)

Sync mode (--sync) runs as a daemon, continuously fetching new bundles.
For one-time sync, use 'plcbundle sync' command instead.`,

		Example: `  # Basic server (read-only, current directory)
  plcbundle server

  # Server with specific directory
  plcbundle server --dir ./my-bundles

  # Live syncing server (daemon mode)
  plcbundle server --sync
  plcbundle server -s

  # Using alias
  plcbundle serve -s

  # Custom port and host
  plcbundle server --port 3000 --host 0.0.0.0

  # Full featured server
  plcbundle server -s --websocket --resolver

  # Fast sync interval
  plcbundle server -s --interval 30s

  # Sync with limit (stop after 1000 bundles)
  plcbundle server -s --max-bundles 1000

  # Public server with all features
  plcbundle serve -s --websocket --resolver --host 0.0.0.0 --port 80`,

		RunE: func(cmd *cobra.Command, args []string) error {
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")

			// Server in sync mode can create repo, read-only mode cannot
			mgr, dir, err := getManager(&ManagerOptions{
				Cmd:      cmd,
				PLCURL:   plcURL,
				AutoInit: syncMode, // Only auto-init if syncing
			})
			if err != nil {
				return err
			}
			defer mgr.Close()

			fmt.Printf("Starting plcbundle HTTP server...\n")
			fmt.Printf("  Directory: %s\n", dir)

			// Build/verify DID index if resolver enabled
			if enableResolver {
				ctx := context.Background()

				built, err := mgr.EnsureDIDIndex(ctx, func(current, total int) {
					if current%100 == 0 || current == total {
						fmt.Printf("  Building DID index: %d/%d (%.1f%%)    \r",
							current, total, float64(current)/float64(total)*100)
						if current == total {
							fmt.Println()
						}
					}
				})

				if err != nil {
					fmt.Fprintf(os.Stderr, "⚠️  DID index error: %v\n", err)
					fmt.Fprintf(os.Stderr, "    DID resolution will not be available\n\n")
				} else if built {
					stats := mgr.GetDIDIndexStats()
					fmt.Printf("  ✓ DID index ready (%s DIDs)\n\n",
						formatNumber(int(stats["total_dids"].(int64))))
				} else {
					stats := mgr.GetDIDIndexStats()
					fmt.Printf("  DID Index: ready (%s DIDs)\n\n",
						formatNumber(int(stats["total_dids"].(int64))))
				}
			}

			addr := fmt.Sprintf("%s:%s", host, port)

			// Display server info
			displayServerInfo(mgr, addr, syncMode, enableWebSocket, enableResolver, plcURL, syncInterval, maxBundles)

			// Setup graceful shutdown
			ctx, cancel := setupServerShutdown(mgr)
			defer cancel()

			// Start sync loop if enabled (always continuous for server)
			if syncMode {
				go runServerSyncLoop(ctx, mgr, syncInterval, maxBundles, verbose)
			}
			mgr.SetQuiet(true)

			// Create and start HTTP server
			serverConfig := &server.Config{
				Addr:            addr,
				SyncMode:        syncMode,
				SyncInterval:    syncInterval,
				EnableWebSocket: enableWebSocket,
				EnableResolver:  enableResolver,
				Version:         GetVersion(),
			}

			srv := server.New(mgr, serverConfig)

			if err := srv.ListenAndServe(); err != nil {
				return fmt.Errorf("server error: %w", err)
			}

			return nil
		},
	}

	// Server flags
	cmd.Flags().StringVar(&port, "port", "8080", "HTTP server port")
	cmd.Flags().StringVar(&host, "host", "127.0.0.1", "HTTP server host (use 0.0.0.0 for all interfaces)")

	// Sync mode flag (consolidated - always means daemon)
	cmd.Flags().BoolVarP(&syncMode, "sync", "s", false, "Enable sync mode (run as daemon, continuously fetch from PLC)")
	cmd.Flags().StringVar(&plcURL, "plc", "https://plc.directory", "PLC directory URL (for sync mode)")
	cmd.Flags().DurationVar(&syncInterval, "interval", 1*time.Minute, "Sync interval (how often to check for new bundles)")
	cmd.Flags().IntVar(&maxBundles, "max-bundles", 0, "Maximum bundles to fetch (0 = unlimited)")

	// Feature flags
	cmd.Flags().BoolVar(&enableWebSocket, "websocket", false, "Enable WebSocket endpoint for streaming")
	cmd.Flags().BoolVar(&enableResolver, "resolver", false, "Enable DID resolution endpoints")

	return cmd
}

// setupServerShutdown sets up signal handling for server
func setupServerShutdown(mgr *bundle.Manager) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Fprintf(os.Stderr, "\n\n⚠️  Shutdown signal received...\n")
		fmt.Fprintf(os.Stderr, "  Saving mempool...\n")

		if err := mgr.GetMempool().Save(); err != nil {
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

// runServerSyncLoop runs the sync loop for server (always continuous)
func runServerSyncLoop(ctx context.Context, mgr *bundle.Manager, interval time.Duration, maxBundles int, verbose bool) {
	logger := &serverLogger{}

	config := &internalsync.SyncLoopConfig{
		Interval:   interval,
		MaxBundles: maxBundles,
		Verbose:    verbose,
		Logger:     logger,
	}

	// Server always runs continuous sync loop
	if err := mgr.RunSyncLoop(ctx, config); err != nil {
		if err != context.Canceled {
			fmt.Fprintf(os.Stderr, "[Sync] Error: %v\n", err)
		}
	}
}

// displayServerInfo shows server configuration
func displayServerInfo(mgr *bundle.Manager, addr string, syncMode, wsEnabled, resolverEnabled bool, plcURL string, syncInterval time.Duration, maxBundles int) {
	fmt.Printf("  Listening: http://%s\n", addr)

	if syncMode {
		fmt.Printf("  Sync: ENABLED (daemon mode)\n")
		fmt.Printf("    PLC URL: %s\n", plcURL)
		fmt.Printf("    Interval: %s\n", syncInterval)
		if maxBundles > 0 {
			fmt.Printf("    Max bundles: %d\n", maxBundles)
		}
	} else {
		fmt.Printf("  Sync: disabled (read-only archive)\n")
		fmt.Printf("    Tip: Use --sync to enable live syncing\n")
	}

	if wsEnabled {
		fmt.Printf("  WebSocket: ENABLED (ws://%s/ws)\n", addr)
	} else {
		fmt.Printf("  WebSocket: disabled\n")
	}

	if resolverEnabled {
		fmt.Printf("  Resolver: ENABLED (/<did> endpoints)\n")
		stats := mgr.GetDIDIndexStats()
		if stats["exists"].(bool) {
			fmt.Printf("    Index: %s DIDs\n", formatNumber(int(stats["total_dids"].(int64))))
		}
	} else {
		fmt.Printf("  Resolver: disabled\n")
	}

	bundleCount := mgr.GetIndex().Count()
	fmt.Printf("  Bundles: %d available\n", bundleCount)

	fmt.Printf("\nPress Ctrl+C to stop\n\n")
}

// serverLogger adapter
type serverLogger struct{}

func (l *serverLogger) Printf(format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", v...)
}

func (l *serverLogger) Println(v ...interface{}) {
	fmt.Fprintln(os.Stderr, v...)
}
