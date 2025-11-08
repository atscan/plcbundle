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

// NewFetcher creates a new fetcher
func NewFetcher(plcClient *plcclient.Client, operations *storage.Operations, logger types.Logger) *Fetcher {
	return &Fetcher{
		plcClient:  plcClient,
		operations: operations,
		logger:     logger,
	}
}

// FetchToMempool fetches operations and returns them
// Returns empty slice + nil error when caught up (incomplete batch received)
func (f *Fetcher) FetchToMempool(
	ctx context.Context,
	afterTime string,
	prevBoundaryCIDs map[string]bool,
	target int,
	quiet bool,
	currentMempoolCount int,
	totalFetchesSoFar int,
) ([]plcclient.PLCOperation, int, error) {

	seenCIDs := make(map[string]bool)

	// Mark previous boundary CIDs as seen
	for cid := range prevBoundaryCIDs {
		seenCIDs[cid] = true
	}

	currentAfter := afterTime
	maxFetches := 20
	var allNewOps []plcclient.PLCOperation
	fetchesMade := 0

	for fetchNum := 0; fetchNum < maxFetches; fetchNum++ {
		fetchesMade++
		remaining := target - len(allNewOps)
		if remaining <= 0 {
			break
		}

		// ✨ SMART BATCH SIZING
		var batchSize int
		switch {
		case remaining <= 50:
			batchSize = 50 // Fetch exactly what we need (with small buffer)
		case remaining <= 100:
			batchSize = 100
		case remaining <= 500:
			batchSize = 200
		default:
			batchSize = 1000
		}

		fetchStart := time.Now()

		if !quiet {
			f.logger.Printf("  Fetch #%d: requesting %d operations (need %d, after: %s)",
				totalFetchesSoFar+fetchesMade, batchSize, remaining, currentAfter[:19])
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
				f.logger.Printf("  No more operations available from PLC (in %s)", fetchDuration)
			}
			return allNewOps, fetchesMade, nil
		}

		// Deduplicate
		beforeDedup := len(allNewOps)
		for _, op := range batch {
			if !seenCIDs[op.CID] {
				seenCIDs[op.CID] = true
				allNewOps = append(allNewOps, op)
			}
		}

		uniqueAdded := len(allNewOps) - beforeDedup
		deduped := len(batch) - uniqueAdded

		// ✨ DETAILED METRICS
		if !quiet {
			opsPerSec := float64(len(batch)) / fetchDuration.Seconds()

			if deduped > 0 {
				f.logger.Printf("  Received %d ops (%d unique, %d dupes) in %s (%.0f ops/sec)",
					len(batch), uniqueAdded, deduped, fetchDuration, opsPerSec)
			} else {
				f.logger.Printf("  Received %d ops in %s (%.0f ops/sec)",
					len(batch), fetchDuration, opsPerSec)
			}
		}

		// Update cursor
		if len(batch) > 0 {
			currentAfter = batch[len(batch)-1].CreatedAt.Format(time.RFC3339Nano)
		}

		// Stop if we got incomplete batch (caught up!)
		if len(batch) < batchSize {
			if !quiet {
				f.logger.Printf("  Received incomplete batch (%d/%d) → caught up to latest",
					len(batch), batchSize)
			}
			return allNewOps, fetchesMade, nil
		}

		// If we have enough, stop
		if len(allNewOps) >= target {
			if !quiet {
				f.logger.Printf("  ✓ Target reached (%d/%d unique ops collected)",
					len(allNewOps), target)
			}
			break
		}
	}

	return allNewOps, fetchesMade, nil
}
