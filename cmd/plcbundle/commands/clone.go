package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	internalsync "tangled.org/atscan.net/plcbundle/internal/sync"
)

// CloneCommand handles the clone subcommand
func CloneCommand(args []string) error {
	fs := flag.NewFlagSet("clone", flag.ExitOnError)
	workers := fs.Int("workers", 4, "number of concurrent download workers")
	verbose := fs.Bool("v", false, "verbose output")
	skipExisting := fs.Bool("skip-existing", true, "skip bundles that already exist locally")
	saveInterval := fs.Duration("save-interval", 5*time.Second, "interval to save index during download")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: plcbundle clone <remote-url> [options]\n\n" +
			"Clone bundles from a remote plcbundle HTTP endpoint\n\n" +
			"Example:\n" +
			"  plcbundle clone https://plc.example.com")
	}

	remoteURL := strings.TrimSuffix(fs.Arg(0), "/")

	// Create manager
	mgr, dir, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	fmt.Printf("Cloning from: %s\n", remoteURL)
	fmt.Printf("Target directory: %s\n", dir)
	fmt.Printf("Workers: %d\n", *workers)
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
		Workers:      *workers,
		SkipExisting: *skipExisting,
		SaveInterval: *saveInterval,
		Verbose:      *verbose,
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
