// internal/sync/syncer.go
package sync

import (
	"context"
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
	// Returns: bundleNumber, indexUpdateTime, error
	FetchAndSaveNextBundle(ctx context.Context, verbose bool, quiet bool) (int, *types.BundleProductionStats, error)
	SaveMempool() error
}

// SyncOnce performs a single sync cycle - fetches until caught up
func SyncOnce(ctx context.Context, mgr SyncManager, config *SyncLoopConfig, verbose bool) (int, error) {
	cycleStart := time.Now()
	startMempool := mgr.GetMempoolCount()

	fetchedCount := 0
	var totalIndexTime time.Duration

	// Keep fetching until caught up (detect by checking if state changes)
	for {
		// Track state before fetch
		bundleBefore := mgr.GetLastBundleNumber()
		mempoolBefore := mgr.GetMempoolCount()

		// Attempt to fetch and save next bundle
		bundleNum, stats, err := mgr.FetchAndSaveNextBundle(ctx, verbose, false)

		// Check if we made any progress
		bundleAfter := mgr.GetLastBundleNumber()
		mempoolAfter := mgr.GetMempoolCount()

		madeProgress := bundleAfter > bundleBefore || mempoolAfter > mempoolBefore

		if err != nil {
			// If no progress and got error → caught up
			if !madeProgress {
				break
			}

			// We added to mempool but couldn't complete bundle yet
			// This is fine, just stop here
			break
		}

		// Success
		fetchedCount++
		totalIndexTime += stats.IndexTime

		// Callback if provided
		if config.OnBundleSynced != nil {
			config.OnBundleSynced(bundleNum, fetchedCount, mempoolAfter, time.Since(cycleStart), totalIndexTime)
		}

		// Small delay between bundles
		time.Sleep(500 * time.Millisecond)

		// Check if we're still making progress
		if !madeProgress {
			break
		}
	}

	// Summary output
	if config.Logger != nil {
		mempoolAfter := mgr.GetMempoolCount()
		addedOps := mempoolAfter - startMempool
		duration := time.Since(cycleStart)
		currentBundle := mgr.GetLastBundleNumber()

		if fetchedCount > 0 {
			if totalIndexTime > 10*time.Millisecond {
				config.Logger.Printf("[Sync] ✓ Bundle %06d | Synced: %d | Mempool: %d (+%d) | %s (index: %s)",
					currentBundle, fetchedCount, mempoolAfter, addedOps,
					duration.Round(time.Millisecond), totalIndexTime.Round(time.Millisecond))
			} else {
				config.Logger.Printf("[Sync] ✓ Bundle %06d | Synced: %d | Mempool: %d (+%d) | %s",
					currentBundle, fetchedCount, mempoolAfter, addedOps, duration.Round(time.Millisecond))
			}
		} else if addedOps > 0 {
			// No bundles but added to mempool
			config.Logger.Printf("[Sync] ✓ Bundle %06d | Mempool: %d (+%d) | %s",
				currentBundle, mempoolAfter, addedOps, duration.Round(time.Millisecond))
		} else {
			// Already up to date
			config.Logger.Printf("[Sync] ✓ Bundle %06d | Up to date | %s",
				currentBundle, duration.Round(time.Millisecond))
		}
	}

	return fetchedCount, nil
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

	// Initial sync
	if config.Logger != nil && config.MaxBundles != 1 {
		config.Logger.Printf("[Sync] Initial sync starting...")
	}

	synced, err := SyncOnce(ctx, mgr, config, config.Verbose)
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
			// Each tick, do one sync cycle (which fetches until caught up)
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
