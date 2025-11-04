package bundle

import (
	"context"
	"fmt"
	"sort"

	"tangled.org/atscan.net/plcbundle/plc"
)

// GetDIDOperationsBundledOnly retrieves operations from bundles only (no mempool)
func (m *Manager) GetDIDOperationsBundledOnly(ctx context.Context, did string, verbose bool) ([]plc.PLCOperation, error) {
	if err := plc.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	// Set verbose mode on index
	if m.didIndex != nil {
		m.didIndex.SetVerbose(verbose)
	}

	// Use index if available
	if m.didIndex != nil && m.didIndex.Exists() {
		if verbose {
			m.logger.Printf("DEBUG: Using DID index for lookup")
		}
		return m.getDIDOperationsIndexed(ctx, did, verbose)
	}

	// Fallback to full scan
	if verbose {
		m.logger.Printf("DEBUG: DID index not available, using full scan")
	}
	m.logger.Printf("Warning: DID index not available, falling back to full scan")
	return m.getDIDOperationsScan(ctx, did)
}

// GetDIDOperationsFromMempool retrieves operations for a DID from mempool only
func (m *Manager) GetDIDOperationsFromMempool(did string) ([]plc.PLCOperation, error) {
	if err := plc.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	if m.mempool == nil {
		return []plc.PLCOperation{}, nil
	}

	// Get all mempool operations (max 10K)
	allMempoolOps, err := m.GetMempoolOperations()
	if err != nil {
		return nil, err
	}

	// Pre-allocate with reasonable capacity
	matchingOps := make([]plc.PLCOperation, 0, 16)

	// Linear scan (fast for 10K operations)
	for _, op := range allMempoolOps {
		if op.DID == did {
			matchingOps = append(matchingOps, op)
		}
	}

	return matchingOps, nil
}

// GetDIDOperations retrieves all operations for a DID (bundles + mempool combined)
func (m *Manager) GetDIDOperations(ctx context.Context, did string, verbose bool) ([]plc.PLCOperation, error) {
	if err := plc.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	// Get bundled operations
	bundledOps, err := m.GetDIDOperationsBundledOnly(ctx, did, verbose)
	if err != nil {
		return nil, err
	}

	// Get mempool operations
	mempoolOps, err := m.GetDIDOperationsFromMempool(did)
	if err != nil {
		return nil, err
	}

	if len(mempoolOps) > 0 && verbose {
		m.logger.Printf("DEBUG: Found %d operations in mempool", len(mempoolOps))
	}

	// Combine and sort
	allOps := append(bundledOps, mempoolOps...)

	sort.Slice(allOps, func(i, j int) bool {
		return allOps[i].CreatedAt.Before(allOps[j].CreatedAt)
	})

	return allOps, nil
}

// getDIDOperationsIndexed uses index for fast lookup (PRIVATE - bundles only)
func (m *Manager) getDIDOperationsIndexed(ctx context.Context, did string, verbose bool) ([]plc.PLCOperation, error) {
	locations, err := m.didIndex.GetDIDLocations(did)
	if err != nil {
		return nil, err
	}

	if len(locations) == 0 {
		return []plc.PLCOperation{}, nil
	}

	// Filter nullified at index level (save loading those bundles!)
	var validLocations []OpLocation
	for _, loc := range locations {
		if !loc.Nullified {
			validLocations = append(validLocations, loc)
		}
	}

	if verbose {
		m.logger.Printf("DEBUG: Filtered %d valid locations (from %d total)",
			len(validLocations), len(locations))
	}

	// Group by bundle
	bundleMap := make(map[uint16][]uint16)
	for _, loc := range validLocations {
		bundleMap[loc.Bundle] = append(bundleMap[loc.Bundle], loc.Position)
	}

	if verbose {
		m.logger.Printf("DEBUG: Loading %d bundles", len(bundleMap))
	}

	// Load operations
	var allOps []plc.PLCOperation
	for bundleNum, positions := range bundleMap {
		bundle, err := m.LoadBundle(ctx, int(bundleNum))
		if err != nil {
			m.logger.Printf("Warning: failed to load bundle %d: %v", bundleNum, err)
			continue
		}

		for _, pos := range positions {
			if int(pos) < len(bundle.Operations) {
				allOps = append(allOps, bundle.Operations[pos])
			}
		}
	}

	if verbose {
		m.logger.Printf("DEBUG: Loaded %d total operations", len(allOps))
	}

	sort.Slice(allOps, func(i, j int) bool {
		return allOps[i].CreatedAt.Before(allOps[j].CreatedAt)
	})

	return allOps, nil
}

// getDIDOperationsScan falls back to full scan (slow) (PRIVATE - bundles only)
func (m *Manager) getDIDOperationsScan(ctx context.Context, did string) ([]plc.PLCOperation, error) {
	var allOps []plc.PLCOperation
	bundles := m.index.GetBundles()

	for _, meta := range bundles {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		bundle, err := m.LoadBundle(ctx, meta.BundleNumber)
		if err != nil {
			m.logger.Printf("Warning: failed to load bundle %d: %v", meta.BundleNumber, err)
			continue
		}

		for _, op := range bundle.Operations {
			if op.DID == did {
				allOps = append(allOps, op)
			}
		}
	}

	sort.Slice(allOps, func(i, j int) bool {
		return allOps[i].CreatedAt.Before(allOps[j].CreatedAt)
	})

	return allOps, nil
}

// GetLatestDIDOperation returns only the most recent non-nullified operation
func (m *Manager) GetLatestDIDOperation(ctx context.Context, did string) (*plc.PLCOperation, error) {
	if err := plc.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	// Check mempool first (most recent operations)
	mempoolOps, err := m.GetDIDOperationsFromMempool(did)
	if err == nil && len(mempoolOps) > 0 {
		// Find latest non-nullified in mempool
		for i := len(mempoolOps) - 1; i >= 0; i-- {
			if !mempoolOps[i].IsNullified() {
				// Found in mempool - but still verify nothing newer in bundles
				// (shouldn't happen with chronological ordering, but be safe)
				bundledLatest, _ := m.getLatestDIDOperationIndexed(ctx, did)
				if bundledLatest == nil || mempoolOps[i].CreatedAt.After(bundledLatest.CreatedAt) {
					return &mempoolOps[i], nil
				}
				return bundledLatest, nil
			}
		}
	}

	// Not in mempool - use index/scan
	if m.didIndex != nil && m.didIndex.Exists() {
		return m.getLatestDIDOperationIndexed(ctx, did)
	}

	// Fallback to full lookup
	ops, err := m.GetDIDOperationsBundledOnly(ctx, did, false)
	if err != nil {
		return nil, err
	}

	// Find latest non-nullified
	for i := len(ops) - 1; i >= 0; i-- {
		if !ops[i].IsNullified() {
			return &ops[i], nil
		}
	}

	return nil, fmt.Errorf("no valid operations found")
}

// getLatestDIDOperationIndexed uses index to find only the latest operation (PRIVATE)
func (m *Manager) getLatestDIDOperationIndexed(ctx context.Context, did string) (*plc.PLCOperation, error) {
	// Get all locations from index
	locations, err := m.didIndex.GetDIDLocations(did)
	if err != nil {
		return nil, err
	}

	if len(locations) == 0 {
		return nil, fmt.Errorf("DID not found")
	}

	// Filter non-nullified and find the latest
	var latestLoc *OpLocation

	for i := range locations {
		if locations[i].Nullified {
			continue
		}

		// Check if this is later than current latest
		if latestLoc == nil {
			latestLoc = &locations[i]
		} else {
			// Compare by bundle, then position
			if locations[i].Bundle > latestLoc.Bundle ||
				(locations[i].Bundle == latestLoc.Bundle && locations[i].Position > latestLoc.Position) {
				latestLoc = &locations[i]
			}
		}
	}

	if latestLoc == nil {
		return nil, fmt.Errorf("no valid operations found (all nullified)")
	}

	// ✨ Load ONLY that ONE operation (not the whole bundle!)
	op, err := m.LoadOperation(ctx, int(latestLoc.Bundle), int(latestLoc.Position))
	if err != nil {
		return nil, fmt.Errorf("failed to load operation at bundle %d position %d: %w",
			latestLoc.Bundle, latestLoc.Position, err)
	}

	return op, nil
}

// BuildDIDIndex builds the complete DID index
func (m *Manager) BuildDIDIndex(ctx context.Context, progressCallback func(current, total int)) error {
	if m.didIndex == nil {
		m.didIndex = NewDIDIndexManager(m.config.BundleDir, m.logger)
	}

	return m.didIndex.BuildIndexFromScratch(ctx, m, progressCallback)
}

// UpdateDIDIndexForBundle updates index when a new bundle is added
func (m *Manager) UpdateDIDIndexForBundle(ctx context.Context, bundle *Bundle) error {
	if m.didIndex == nil {
		return nil // Index not initialized
	}

	return m.didIndex.UpdateIndexForBundle(ctx, bundle)
}

// GetDIDIndexStats returns DID index statistics (including mempool)
func (m *Manager) GetDIDIndexStats() map[string]interface{} {
	if m.didIndex == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	stats := m.didIndex.GetStats()
	stats["enabled"] = true
	stats["exists"] = m.didIndex.Exists()

	indexedDIDs := stats["total_dids"].(int64)

	// Get unique DIDs from mempool
	mempoolDIDCount := int64(0)
	if m.mempool != nil {
		mempoolStats := m.GetMempoolStats()
		if didCount, ok := mempoolStats["did_count"].(int); ok {
			mempoolDIDCount = int64(didCount)
		}
	}

	// Add separate fields for clarity
	stats["indexed_dids"] = indexedDIDs                 // DIDs in bundles
	stats["mempool_dids"] = mempoolDIDCount             // DIDs in mempool
	stats["total_dids"] = indexedDIDs + mempoolDIDCount // ← Combined total

	return stats
}
