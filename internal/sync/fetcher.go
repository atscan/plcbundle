package sync

import (
	"context"
	"fmt"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/storage"
	"tangled.org/atscan.net/plcbundle/internal/types"
	"tangled.org/atscan.net/plcbundle/plcclient"
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
// Returns: operations, error
func (f *Fetcher) FetchToMempool(
	ctx context.Context,
	afterTime string,
	prevBoundaryCIDs map[string]bool,
	target int,
	quiet bool,
	currentMempoolCount int,
) ([]plcclient.PLCOperation, error) {

	seenCIDs := make(map[string]bool)

	// Mark previous boundary CIDs as seen
	for cid := range prevBoundaryCIDs {
		seenCIDs[cid] = true
	}

	currentAfter := afterTime
	maxFetches := 20
	var allNewOps []plcclient.PLCOperation

	for fetchNum := 0; fetchNum < maxFetches; fetchNum++ {
		// Calculate batch size
		remaining := target - len(allNewOps)
		if remaining <= 0 {
			break
		}

		batchSize := 1000
		if remaining < 500 {
			batchSize = 200
		}

		if !quiet {
			f.logger.Printf("  Fetch #%d: requesting %d operations",
				fetchNum+1, batchSize)
		}

		batch, err := f.plcClient.Export(ctx, plcclient.ExportOptions{
			Count: batchSize,
			After: currentAfter,
		})
		if err != nil {
			return allNewOps, fmt.Errorf("export failed: %w", err)
		}

		if len(batch) == 0 {
			if !quiet {
				f.logger.Printf("  No more operations available from PLC")
			}
			if len(allNewOps) > 0 {
				return allNewOps, nil
			}
			return nil, fmt.Errorf("no operations available")
		}

		// Deduplicate
		for _, op := range batch {
			if !seenCIDs[op.CID] {
				seenCIDs[op.CID] = true
				allNewOps = append(allNewOps, op)
			}
		}

		// Update cursor
		if len(batch) > 0 {
			currentAfter = batch[len(batch)-1].CreatedAt.Format(time.RFC3339Nano)
		}

		// Stop if we got less than requested
		if len(batch) < batchSize {
			if !quiet {
				f.logger.Printf("  Received incomplete batch (%d/%d), caught up to latest", len(batch), batchSize)
			}
			break
		}
	}

	if len(allNewOps) > 0 {
		if !quiet {
			f.logger.Printf("✓ Fetch complete: %d operations", len(allNewOps))
		}
		return allNewOps, nil
	}

	return nil, fmt.Errorf("no new operations added")
}
