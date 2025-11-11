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
	mempool MempoolInterface,
	totalFetchesSoFar int,
) ([]plcclient.PLCOperation, int, error) {

	seenCIDs := make(map[string]bool)

	// Initialize current boundaries from previous bundle (or empty if first fetch)
	currentBoundaryCIDs := prevBoundaryCIDs
	if currentBoundaryCIDs == nil {
		currentBoundaryCIDs = make(map[string]bool)
	}

	// Mark boundary CIDs as seen to prevent re-inclusion
	for cid := range currentBoundaryCIDs {
		seenCIDs[cid] = true
	}

	if !quiet && len(currentBoundaryCIDs) > 0 {
		f.logger.Printf("  Starting with %d boundary CIDs from previous bundle", len(currentBoundaryCIDs))
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

		// CRITICAL: Strip boundary duplicates using current boundaries
		batch = f.operations.StripBoundaryDuplicates(
			batch,
			currentAfter,
			currentBoundaryCIDs,
		)

		afterStripSize := len(batch)
		strippedCount := originalBatchSize - afterStripSize

		if !quiet && strippedCount > 0 {
			f.logger.Printf("  Stripped %d boundary duplicates from fetch", strippedCount)
		}

		// Collect new ops (not in seenCIDs)
		beforeDedup := len(allNewOps)
		var batchNewOps []plcclient.PLCOperation

		for _, op := range batch {
			if !seenCIDs[op.CID] {
				batchNewOps = append(batchNewOps, op)
			}
		}

		uniqueInBatch := len(batchNewOps)
		dupesFiltered := afterStripSize - uniqueInBatch
		totalDupes += dupesFiltered + strippedCount

		// Try to add to mempool
		if uniqueInBatch > 0 && mempool != nil {
			_, addErr := mempool.Add(batchNewOps)

			if addErr != nil {
				// Add failed - don't mark as seen
				if !quiet {
					f.logger.Printf("  ❌ Mempool add failed: %v", addErr)
				}
				mempool.Save()
				return allNewOps, fetchesMade, fmt.Errorf("mempool add failed: %w", addErr)
			}

			// Success - mark as seen
			for _, op := range batchNewOps {
				seenCIDs[op.CID] = true
			}
			allNewOps = append(allNewOps, batchNewOps...)

			uniqueAdded := len(allNewOps) - beforeDedup

			if !quiet {
				opsPerSec := float64(originalBatchSize) / fetchDuration.Seconds()
				if dupesFiltered+strippedCount > 0 {
					f.logger.Printf("  → +%d unique (%d dupes, %d boundary) in %s • Running: %d/%d (%.0f ops/sec)",
						uniqueAdded, dupesFiltered, strippedCount, fetchDuration, len(allNewOps), target, opsPerSec)
				} else {
					f.logger.Printf("  → +%d unique in %s • Running: %d/%d (%.0f ops/sec)",
						uniqueAdded, fetchDuration, len(allNewOps), target, opsPerSec)
				}
			}

			// CRITICAL: Calculate NEW boundary CIDs from this fetch for next iteration
			if len(batch) > 0 {
				boundaryTime, newBoundaryCIDs := f.operations.GetBoundaryCIDs(batch)
				currentBoundaryCIDs = newBoundaryCIDs
				currentAfter = boundaryTime.Format(time.RFC3339Nano)

				if !quiet && len(newBoundaryCIDs) > 1 {
					f.logger.Printf("  Updated boundaries: %d CIDs at %s",
						len(newBoundaryCIDs), currentAfter[:19])
				}
			}

			// Save if threshold met
			if err := mempool.SaveIfNeeded(); err != nil {
				f.logger.Printf("  Warning: failed to save mempool: %v", err)
			}

		} else if uniqueInBatch > 0 {
			// No mempool - just collect
			for _, op := range batchNewOps {
				seenCIDs[op.CID] = true
			}
			allNewOps = append(allNewOps, batchNewOps...)

			// Still update boundaries even without mempool
			if len(batch) > 0 {
				boundaryTime, newBoundaryCIDs := f.operations.GetBoundaryCIDs(batch)
				currentBoundaryCIDs = newBoundaryCIDs
				currentAfter = boundaryTime.Format(time.RFC3339Nano)
			}
		}

		// Check if incomplete batch (caught up)
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
