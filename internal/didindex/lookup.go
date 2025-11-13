package didindex

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

// GetDIDOperations retrieves all operations for a DID WITH location metadata
// Returns operations with bundle/position info (includes nullified operations)
func (dim *Manager) GetDIDOperations(ctx context.Context, did string, provider BundleProvider) ([]OpLocationWithOperation, error) {
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
		return []OpLocationWithOperation{}, nil
	}

	// Group by bundle
	bundleMap := make(map[uint16][]OpLocation)
	for _, loc := range locations {
		bundleMap[loc.Bundle()] = append(bundleMap[loc.Bundle()], loc)
	}

	var results []OpLocationWithOperation
	for bundleNum, locs := range bundleMap {
		positions := make([]int, len(locs))
		for i, l := range locs {
			positions[i] = l.PositionInt()
		}
		opsMap, err := provider.LoadOperations(ctx, int(bundleNum), positions)
		if err != nil {
			dim.logger.Printf("Warning: failed to load bundle %d: %v", bundleNum, err)
			continue
		}
		for i, l := range locs {
			if op, ok := opsMap[positions[i]]; ok {
				results = append(results, OpLocationWithOperation{
					Operation: *op,
					Bundle:    l.BundleInt(),
					Position:  l.PositionInt(),
				})
			}
		}
	}

	// Sort by time
	sort.Slice(results, func(i, j int) bool {
		return results[i].Operation.CreatedAt.Before(results[j].Operation.CreatedAt)
	})

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

// BatchGetDIDLocations retrieves locations for multiple DIDs efficiently
// Returns map[did][]OpLocation - only locations, no operation loading
func (dim *Manager) BatchGetDIDLocations(dids []string) (map[string][]OpLocation, error) {
	if !dim.Exists() {
		return nil, fmt.Errorf("DID index not available")
	}

	// Group DIDs by shard to minimize shard loads
	type shardQuery struct {
		shardNum    uint8
		identifiers []string
		didMap      map[string]string // identifier -> original DID
	}

	shardQueries := make(map[uint8]*shardQuery)

	for _, did := range dids {
		identifier, err := extractDIDIdentifier(did)
		if err != nil {
			continue
		}

		shardNum := dim.calculateShard(identifier)

		if shardQueries[shardNum] == nil {
			shardQueries[shardNum] = &shardQuery{
				shardNum:    shardNum,
				identifiers: make([]string, 0),
				didMap:      make(map[string]string),
			}
		}

		sq := shardQueries[shardNum]
		sq.identifiers = append(sq.identifiers, identifier)
		sq.didMap[identifier] = did
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Batch lookup: %d DIDs across %d shards", len(dids), len(shardQueries))
	}

	// Process each shard (load once, search multiple times)
	results := make(map[string][]OpLocation)
	var mu sync.Mutex

	var wg sync.WaitGroup
	for _, sq := range shardQueries {
		wg.Add(1)
		go func(query *shardQuery) {
			defer wg.Done()

			// Load shard once
			shard, err := dim.loadShard(query.shardNum)
			if err != nil {
				if dim.verbose {
					dim.logger.Printf("DEBUG: Failed to load shard %02x: %v", query.shardNum, err)
				}
				return
			}
			defer dim.releaseShard(shard)

			if shard.data == nil {
				return
			}

			// Search for all identifiers in this shard
			for _, identifier := range query.identifiers {
				locations := dim.searchShard(shard, identifier)
				if len(locations) > 0 {
					originalDID := query.didMap[identifier]
					mu.Lock()
					results[originalDID] = locations
					mu.Unlock()
				}
			}
		}(sq)
	}

	wg.Wait()

	return results, nil
}
