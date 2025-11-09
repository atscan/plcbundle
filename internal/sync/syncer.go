// internal/sync/syncer.go
package sync

import (
	"context"
	"fmt"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/types"
)

// SyncLoopConfig configures continuous syncing
type SyncLoopConfig struct {
	Interval       time.Duration
	MaxBundles     int // 0 = unlimited
	Verbose        bool
	Logger         types.Logger
	OnBundleSynced func(bundleNum int, fetchedCount int, mempoolCount int, duration time.Duration, indexTime time.Duration)
}

// DefaultSyncLoopConfig returns default configuration
func DefaultSyncLoopConfig() *SyncLoopConfig {
	return &SyncLoopConfig{
		Interval:   1 * time.Minute,
		MaxBundles: 0,
		Verbose:    false,
	}
}

// SyncManager is the minimal interface needed for syncing
type SyncManager interface {
	GetLastBundleNumber() int
	GetMempoolCount() int
	FetchNextBundle(ctx context.Context, quiet bool) (int, error) // returns bundle number
	SaveBundle(ctx context.Context, bundleNum int, quiet bool) (time.Duration, error)
	SaveMempool() error
}

// RunSyncLoop performs continuous syncing
func RunSyncLoop(ctx context.Context, mgr SyncManager, config *SyncLoopConfig) error {
	if config == nil {
		config = DefaultSyncLoopConfig()
	}

	if config.Interval <= 0 {
		config.Interval = 1 * time.Minute
	}

	bundlesSynced := 0

	// Initial sync - always show detailed progress
	if config.Logger != nil {
		config.Logger.Printf("[Sync] Initial sync starting...")
	}

	synced, err := SyncOnce(ctx, mgr, config, true) // Force verbose for initial
	if err != nil {
		return err
	}
	bundlesSynced += synced

	// Check if reached limit
	if config.MaxBundles > 0 && bundlesSynced >= config.MaxBundles {
		if config.Logger != nil {
			config.Logger.Printf("[Sync] Reached max bundles limit (%d)", config.MaxBundles)
		}
		return nil
	}

	if config.Logger != nil {
		config.Logger.Printf("[Sync] Loop started (interval: %s)", config.Interval)
	}

	ticker := time.NewTicker(config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if config.Logger != nil {
				config.Logger.Printf("[Sync] Stopped (total synced: %d bundles)", bundlesSynced)
			}
			return ctx.Err()

		case <-ticker.C:
			synced, err := SyncOnce(ctx, mgr, config, config.Verbose)
			if err != nil {
				if config.Logger != nil {
					config.Logger.Printf("[Sync] Error: %v", err)
				}
				continue
			}

			bundlesSynced += synced

			// Check if reached limit
			if config.MaxBundles > 0 && bundlesSynced >= config.MaxBundles {
				if config.Logger != nil {
					config.Logger.Printf("[Sync] Reached max bundles limit (%d)", config.MaxBundles)
				}
				return nil
			}
		}
	}
}

// SyncOnce performs a single sync cycle (exported for single-shot syncing)
func SyncOnce(ctx context.Context, mgr SyncManager, config *SyncLoopConfig, verbose bool) (int, error) {
	cycleStart := time.Now()

	startBundle := mgr.GetLastBundleNumber() + 1
	mempoolBefore := mgr.GetMempoolCount()
	fetchedCount := 0
	var totalIndexTime time.Duration

	// Keep fetching until caught up
	for {
		// quiet = !verbose
		bundleNum, err := mgr.FetchNextBundle(ctx, !verbose)
		if err != nil {
			if isEndOfDataError(err) {
				break
			}
			return fetchedCount, fmt.Errorf("fetch failed: %w", err)
		}

		// Save bundle and track index update time
		indexTime, err := mgr.SaveBundle(ctx, bundleNum, !verbose)
		if err != nil {
			return fetchedCount, fmt.Errorf("save failed: %w", err)
		}

		fetchedCount++
		totalIndexTime += indexTime

		// Callback if provided
		if config.OnBundleSynced != nil {
			mempoolAfter := mgr.GetMempoolCount()
			config.OnBundleSynced(bundleNum, fetchedCount, mempoolAfter, time.Since(cycleStart), totalIndexTime)
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Summary output
	if config.Logger != nil {
		mempoolAfter := mgr.GetMempoolCount()
		addedOps := mempoolAfter - mempoolBefore
		duration := time.Since(cycleStart)

		currentBundle := startBundle + fetchedCount - 1
		if fetchedCount == 0 {
			currentBundle = startBundle - 1
		}

		if fetchedCount > 0 {
			if totalIndexTime > 10*time.Millisecond {
				config.Logger.Printf("[Sync] ✓ Bundle %06d | Synced: %d | Mempool: %d (+%d) | %s (index: %s)",
					currentBundle, fetchedCount, mempoolAfter, addedOps,
					duration.Round(time.Millisecond), totalIndexTime.Round(time.Millisecond))
			} else {
				config.Logger.Printf("[Sync] ✓ Bundle %06d | Synced: %d | Mempool: %d (+%d) | %s",
					currentBundle, fetchedCount, mempoolAfter, addedOps, duration.Round(time.Millisecond))
			}
		} else {
			config.Logger.Printf("[Sync] ✓ Bundle %06d | Up to date | Mempool: %d (+%d) | %s",
				currentBundle, mempoolAfter, addedOps, duration.Round(time.Millisecond))
		}
	}

	return fetchedCount, nil
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
		"reached latest data",
		"caught up to latest")
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
