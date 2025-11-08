package sync

import (
	"context"
	"fmt"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/storage"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

// Fetcher handles fetching operations from PLC directory
type Fetcher struct {
	plcClient  *plcclient.Client
	operations *storage.Operations
	logger     types.Logger
}

// MempoolInterface defines what we need from mempool
type MempoolInterface interface {
	Add(ops []plcclient.PLCOperation) (int, error)
	Save() error
	SaveIfNeeded() error
	Count() int
	GetLastTime() string
}

// NewFetcher creates a new fetcher
func NewFetcher(plcClient *plcclient.Client, operations *storage.Operations, logger types.Logger) *Fetcher {
	return &Fetcher{
		plcClient:  plcClient,
		operations: operations,
		logger:     logger,
	}
}

// FetchToMempool fetches operations and adds them to mempool (with auto-save)
func (f *Fetcher) FetchToMempool(
	ctx context.Context,
	afterTime string,
	prevBoundaryCIDs map[string]bool,
	target int,
	quiet bool,
	mempool MempoolInterface, // NEW: pass mempool directly
	totalFetchesSoFar int,
) ([]plcclient.PLCOperation, int, error) {

	seenCIDs := make(map[string]bool)

	// Mark previous boundary CIDs as seen
	for cid := range prevBoundaryCIDs {
		seenCIDs[cid] = true
	}

	if !quiet && len(prevBoundaryCIDs) > 0 {
		f.logger.Printf("  Tracking %d boundary CIDs from previous bundle", len(prevBoundaryCIDs))
	}

	currentAfter := afterTime
	maxFetches := 20
	var allNewOps []plcclient.PLCOperation
	fetchesMade := 0
	totalReceived := 0
	totalDupes := 0

	for fetchNum := 0; fetchNum < maxFetches; fetchNum++ {
		fetchesMade++
		remaining := target - len(allNewOps)
		if remaining <= 0 {
			break
		}

		// Smart batch sizing
		var batchSize int
		switch {
		case remaining <= 50:
			batchSize = 50
		case remaining <= 100:
			batchSize = 100
		case remaining <= 500:
			batchSize = 200
		default:
			batchSize = 1000
		}

		fetchStart := time.Now()

		if !quiet {
			f.logger.Printf("  Fetch #%d: requesting %d (need %d more, have %d/%d)",
				totalFetchesSoFar+fetchesMade, batchSize, remaining, len(allNewOps), target)
		}

		batch, err := f.plcClient.Export(ctx, plcclient.ExportOptions{
			Count: batchSize,
			After: currentAfter,
		})

		fetchDuration := time.Since(fetchStart)

		if err != nil {
			return allNewOps, fetchesMade, fmt.Errorf("export failed: %w", err)
		}

		if len(batch) == 0 {
			if !quiet {
				f.logger.Printf("  No more operations available (in %s)", fetchDuration)
			}
			return allNewOps, fetchesMade, nil
		}

		originalBatchSize := len(batch)
		totalReceived += originalBatchSize

		// Deduplicate
		beforeDedup := len(allNewOps)
		for _, op := range batch {
			if !seenCIDs[op.CID] {
				seenCIDs[op.CID] = true
				allNewOps = append(allNewOps, op)
			}
		}

		uniqueAdded := len(allNewOps) - beforeDedup
		dupesFiltered := originalBatchSize - uniqueAdded
		totalDupes += dupesFiltered

		// Show fetch result with running totals
		if !quiet {
			opsPerSec := float64(originalBatchSize) / fetchDuration.Seconds()

			if dupesFiltered > 0 {
				f.logger.Printf("  → +%d unique (%d dupes) in %s • Running: %d/%d (%.0f ops/sec)",
					uniqueAdded, dupesFiltered, fetchDuration, len(allNewOps), target, opsPerSec)
			} else {
				f.logger.Printf("  → +%d unique in %s • Running: %d/%d (%.0f ops/sec)",
					uniqueAdded, fetchDuration, len(allNewOps), target, opsPerSec)
			}
		}

		// ✨ ADD TO MEMPOOL AND SAVE after each fetch
		if uniqueAdded > 0 && mempool != nil {
			added, addErr := mempool.Add(allNewOps[beforeDedup:])
			if addErr != nil {
				// Force save before returning error
				mempool.Save()
				return allNewOps, fetchesMade, fmt.Errorf("mempool add failed: %w", addErr)
			}

			// ✨ Save only if threshold met
			if err := mempool.SaveIfNeeded(); err != nil {
				f.logger.Printf("  Warning: failed to save mempool: %v", err)
			}

			if !quiet && added > 0 {
				cursor := mempool.GetLastTime()
				f.logger.Printf("  Added to mempool: %d ops (total: %d, cursor: %s)",
					added, mempool.Count(), cursor[:19])
			}
		}

		// Update cursor
		if len(batch) > 0 {
			currentAfter = batch[len(batch)-1].CreatedAt.Format(time.RFC3339Nano)
		}

		// Check completeness
		if originalBatchSize < batchSize {
			if !quiet {
				f.logger.Printf("  Incomplete batch (%d/%d) → caught up", originalBatchSize, batchSize)
			}
			return allNewOps, fetchesMade, nil
		}

		if len(allNewOps) >= target {
			break
		}
	}

	// Summary
	if !quiet && fetchesMade > 0 {
		dedupRate := 0.0
		if totalReceived > 0 {
			dedupRate = float64(totalDupes) / float64(totalReceived) * 100
		}
		f.logger.Printf("  ✓ Collected %d unique ops from %d fetches (%.1f%% dedup)",
			len(allNewOps), fetchesMade, dedupRate)
	}

	return allNewOps, fetchesMade, nil
}
