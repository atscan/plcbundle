package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	internalsync "tangled.org/atscan.net/plcbundle/internal/sync"
)

func NewCloneCommand() *cobra.Command {
	var (
		workers int
		resume  bool
	)

	cmd := &cobra.Command{
		Use:   "clone <remote-url> [directory]",
		Short: "Clone bundles from remote HTTP endpoint",
		Long: `Clone bundles from remote plcbundle HTTP endpoint

Download all bundles from a remote plcbundle server to your local
repository. Similar to 'git clone' - creates a complete copy of
the remote repository.

If directory is not specified, bundles will be cloned into './bundles'

The clone process:
  1. Fetches remote index
  2. Downloads missing bundles in parallel
  3. Verifies hashes
  4. Updates local index
  5. Can be interrupted and resumed safely`,

		Args: cobra.RangeArgs(1, 2),

		Example: `  # Clone into default 'bundles' directory
  plcbundle clone https://plc.example.com
  
  # Clone into specific directory
  plcbundle clone https://plc.example.com my-plc-data
  
  # Clone with more parallel workers (faster)
  plcbundle clone https://plc.example.com --workers 8
  
  # Resume interrupted clone
  plcbundle clone https://plc.example.com --resume
  
  # Verbose output (shows each bundle)
  plcbundle clone https://plc.example.com my-bundles -v`,

		RunE: func(cmd *cobra.Command, args []string) error {
			remoteURL := strings.TrimSuffix(args[0], "/")

			targetDir := "bundles"
			if len(args) > 1 {
				targetDir = args[1]
			}

			// Get global verbose flag
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")

			return runClone(remoteURL, targetDir, cloneOptions{
				workers: workers,
				verbose: verbose,
				resume:  resume,
			})
		},
	}

	// Local flags
	cmd.Flags().IntVarP(&workers, "workers", "w", 4,
		"Number of parallel download workers (higher = faster, more bandwidth)")
	cmd.Flags().BoolVarP(&resume, "resume", "r", false,
		"Resume interrupted clone (automatically skips existing bundles)")

	return cmd
}

type cloneOptions struct {
	workers int
	verbose bool
	resume  bool
}

func runClone(remoteURL string, targetDir string, opts cloneOptions) error {
	// Create target directory if it doesn't exist
	absDir, err := filepath.Abs(targetDir)
	if err != nil {
		return fmt.Errorf("invalid directory path: %w", err)
	}

	// Clone creates new repository in specific directory
	mgr, dir, err := getManager(&ManagerOptions{
		Dir:      absDir,
		PLCURL:   "https://plc.directory",
		AutoInit: true,
	})
	if err != nil {
		return err
	}
	defer mgr.Close()

	fmt.Printf("Cloning from: %s\n", remoteURL)
	fmt.Printf("Target directory: %s\n", dir)
	fmt.Printf("Workers: %d\n", opts.workers)

	if opts.resume {
		fmt.Printf("Mode: resume (skipping existing bundles)\n")
	}

	fmt.Printf("(Press Ctrl+C to safely interrupt - progress will be saved)\n\n")

	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Set up progress bar
	var progress *ui.ProgressBar
	var progressMu sync.Mutex
	progressActive := true

	go func() {
		<-sigChan
		progressMu.Lock()
		progressActive = false
		if progress != nil {
			fmt.Println()
		}
		progressMu.Unlock()

		fmt.Printf("\n⚠️  Interrupt received! Finishing current downloads and saving progress...\n")
		cancel()
	}()

	// Clone with library
	result, err := mgr.CloneFromRemote(ctx, internalsync.CloneOptions{
		RemoteURL:    remoteURL,
		Workers:      opts.workers,
		SkipExisting: opts.resume,
		SaveInterval: 5 * time.Second,
		Verbose:      opts.verbose,
		ProgressFunc: func(downloaded, total int, bytesDownloaded, bytesTotal int64) {
			progressMu.Lock()
			defer progressMu.Unlock()

			if !progressActive {
				return
			}

			if progress == nil {
				progress = ui.NewProgressBarWithBytes(total, bytesTotal)
			}
			progress.SetWithBytes(downloaded, bytesDownloaded)
		},
	})

	// Ensure progress is stopped
	progressMu.Lock()
	progressActive = false
	if progress != nil {
		progress.Finish()
	}
	progressMu.Unlock()

	if err != nil {
		return fmt.Errorf("clone failed: %w", err)
	}

	// Display results
	if result.Interrupted {
		fmt.Printf("⚠️  Download interrupted by user\n")
	} else {
		fmt.Printf("\n✓ Clone complete in %s\n", result.Duration.Round(time.Millisecond))
	}

	fmt.Printf("\nResults:\n")
	fmt.Printf("  Remote bundles: %d\n", result.RemoteBundles)
	if result.Skipped > 0 {
		fmt.Printf("  Skipped (existing): %d\n", result.Skipped)
	}
	fmt.Printf("  Downloaded: %d\n", result.Downloaded)
	if result.Failed > 0 {
		fmt.Printf("  Failed: %d\n", result.Failed)
	}
	fmt.Printf("  Total size: %s\n", formatBytes(result.TotalBytes))

	if result.Duration.Seconds() > 0 && result.Downloaded > 0 {
		mbPerSec := float64(result.TotalBytes) / result.Duration.Seconds() / (1024 * 1024)
		bundlesPerSec := float64(result.Downloaded) / result.Duration.Seconds()
		fmt.Printf("  Average speed: %.1f MB/s (%.1f bundles/s)\n", mbPerSec, bundlesPerSec)
	}

	if result.Failed > 0 {
		fmt.Printf("\n⚠️  Failed bundles: ")
		for i, num := range result.FailedBundles {
			if i > 0 {
				fmt.Printf(", ")
			}
			if i > 10 {
				fmt.Printf("... and %d more", len(result.FailedBundles)-10)
				break
			}
			fmt.Printf("%06d", num)
		}
		fmt.Printf("\nRe-run the clone command to retry failed bundles.\n")
		return fmt.Errorf("clone completed with errors")
	}

	if result.Interrupted {
		fmt.Printf("\n✓ Progress saved. Re-run the clone command to resume.\n")
		return fmt.Errorf("clone interrupted")
	}

	fmt.Printf("\n✓ Clone complete!\n")
	return nil
}
