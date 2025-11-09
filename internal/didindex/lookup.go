package didindex

import (
	"context"
	"fmt"
	"sort"

	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

// GetDIDOperations retrieves all operations for a DID from bundles
func (dim *Manager) GetDIDOperations(ctx context.Context, did string, provider BundleProvider) ([]plcclient.PLCOperation, error) {
	if err := plcclient.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	if !dim.Exists() {
		return nil, fmt.Errorf("DID index not available - run 'plcbundle index build' to enable DID lookups")
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Using DID index for lookup")
	}

	locations, err := dim.GetDIDLocations(did)
	if err != nil {
		return nil, err
	}

	if len(locations) == 0 {
		return []plcclient.PLCOperation{}, nil
	}

	// Filter nullified
	var validLocations []OpLocation
	for _, loc := range locations {
		if !loc.Nullified() {
			validLocations = append(validLocations, loc)
		}
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Filtered %d valid locations (from %d total)",
			len(validLocations), len(locations))
	}

	if len(validLocations) == 1 {
		loc := validLocations[0]
		op, err := provider.LoadOperation(ctx, loc.BundleInt(), loc.PositionInt())
		if err != nil {
			return nil, err
		}
		return []plcclient.PLCOperation{*op}, nil
	}

	// For multiple operations: group by bundle to minimize bundle loads
	bundleMap := make(map[uint16][]uint16)
	for _, loc := range validLocations {
		bundleMap[loc.Bundle()] = append(bundleMap[loc.Bundle()], loc.Position())
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Loading from %d bundle(s)", len(bundleMap))
	}

	// Load operations
	var allOps []plcclient.PLCOperation
	for bundleNum, positions := range bundleMap {
		// Optimization: If single position from bundle, use LoadOperation
		if len(positions) == 1 {
			op, err := provider.LoadOperation(ctx, int(bundleNum), int(positions[0]))
			if err != nil {
				dim.logger.Printf("Warning: failed to load operation at bundle %d position %d: %v",
					bundleNum, positions[0], err)
				continue
			}
			allOps = append(allOps, *op)
		} else {
			// Multiple positions: load full bundle
			bundle, err := provider.LoadBundleForDIDIndex(ctx, int(bundleNum))
			if err != nil {
				dim.logger.Printf("Warning: failed to load bundle %d: %v", bundleNum, err)
				continue
			}

			for _, pos := range positions {
				if int(pos) < len(bundle.Operations) {
					allOps = append(allOps, bundle.Operations[pos])
				}
			}
		}
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Loaded %d total operations", len(allOps))
	}

	// Sort by time
	sort.Slice(allOps, func(i, j int) bool {
		return allOps[i].CreatedAt.Before(allOps[j].CreatedAt)
	})

	return allOps, nil
}

// GetDIDOperationsWithLocations returns operations with their bundle/position metadata
func (dim *Manager) GetDIDOperationsWithLocations(ctx context.Context, did string, provider BundleProvider) ([]OpLocationWithOperation, error) {
	if err := plcclient.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	if !dim.Exists() {
		return nil, fmt.Errorf("DID index not available - run 'plcbundle index build' to enable DID lookups")
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Using DID index for lookup with locations")
	}

	locations, err := dim.GetDIDLocations(did)
	if err != nil {
		return nil, err
	}

	if len(locations) == 0 {
		return []OpLocationWithOperation{}, nil
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Found %d locations in index", len(locations))
	}

	// Group by bundle
	bundleMap := make(map[uint16][]OpLocation)
	for _, loc := range locations {
		bundleMap[loc.Bundle()] = append(bundleMap[loc.Bundle()], loc)
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Loading from %d bundle(s)", len(bundleMap))
	}

	var results []OpLocationWithOperation
	for bundleNum, locs := range bundleMap {
		bundle, err := provider.LoadBundleForDIDIndex(ctx, int(bundleNum))
		if err != nil {
			dim.logger.Printf("Warning: failed to load bundle %d: %v", bundleNum, err)
			continue
		}

		for _, loc := range locs {
			if loc.PositionInt() >= len(bundle.Operations) {
				continue
			}

			op := bundle.Operations[loc.Position()]
			results = append(results, OpLocationWithOperation{
				Operation: op,
				Bundle:    loc.BundleInt(),
				Position:  loc.PositionInt(),
			})
		}
	}

	// Sort by time
	sort.Slice(results, func(i, j int) bool {
		return results[i].Operation.CreatedAt.Before(results[j].Operation.CreatedAt)
	})

	if dim.verbose {
		dim.logger.Printf("DEBUG: Loaded %d total operations", len(results))
	}

	return results, nil
}

// GetLatestDIDOperation returns the most recent non-nullified operation
func (dim *Manager) GetLatestDIDOperation(ctx context.Context, did string, provider BundleProvider) (*plcclient.PLCOperation, error) {
	if err := plcclient.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	if !dim.Exists() {
		return nil, fmt.Errorf("DID index not available - run 'plcbundle index build' to enable DID lookups")
	}

	locations, err := dim.GetDIDLocations(did)
	if err != nil {
		return nil, err
	}

	if len(locations) == 0 {
		return nil, fmt.Errorf("DID not found")
	}

	// Find latest non-nullified location
	var latestLoc *OpLocation
	for i := range locations {
		if locations[i].Nullified() {
			continue
		}

		if latestLoc == nil {
			latestLoc = &locations[i]
		} else {
			if locations[i].Bundle() > latestLoc.Bundle() ||
				(locations[i].Bundle() == latestLoc.Bundle() && locations[i].Position() > latestLoc.Position()) {
				latestLoc = &locations[i]
			}
		}
	}

	if latestLoc == nil {
		return nil, fmt.Errorf("no valid operations found (all nullified)")
	}

	// Load ONLY the specific operation (efficient!)
	return provider.LoadOperation(ctx, latestLoc.BundleInt(), latestLoc.PositionInt())
}
