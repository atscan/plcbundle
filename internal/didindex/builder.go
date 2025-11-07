package didindex

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// newShardBuilder creates a new shard builder
func newShardBuilder() *ShardBuilder {
	return &ShardBuilder{
		entries: make(map[string][]OpLocation),
	}
}

// add adds a location to the shard
func (sb *ShardBuilder) add(identifier string, bundle uint16, position uint16, nullified bool) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	sb.entries[identifier] = append(sb.entries[identifier], OpLocation{
		Bundle:    bundle,
		Position:  position,
		Nullified: nullified,
	})
}

// BuildIndexFromScratch builds index with controlled memory usage
func (dim *Manager) BuildIndexFromScratch(ctx context.Context, mgr BundleProvider, progressCallback func(current, total int)) error {
	dim.indexMu.Lock()
	defer dim.indexMu.Unlock()

	dim.logger.Printf("Building DID index from scratch (memory-efficient mode)...")

	bundles := mgr.GetBundleIndex().GetBundles()
	if len(bundles) == 0 {
		return fmt.Errorf("no bundles to index")
	}

	// Create temporary shard files
	tempShards := make([]*os.File, DID_SHARD_COUNT)
	for i := 0; i < DID_SHARD_COUNT; i++ {
		tempPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.tmp", i))
		f, err := os.Create(tempPath)
		if err != nil {
			for j := 0; j < i; j++ {
				tempShards[j].Close()
				os.Remove(filepath.Join(dim.shardDir, fmt.Sprintf("%02x.tmp", j)))
			}
			return fmt.Errorf("failed to create temp shard: %w", err)
		}
		tempShards[i] = f
	}

	dim.logger.Printf("Pass 1/2: Scanning %d bundles...", len(bundles))

	// Stream all operations to temp files
	for i, meta := range bundles {
		select {
		case <-ctx.Done():
			for _, f := range tempShards {
				f.Close()
				os.Remove(f.Name())
			}
			return ctx.Err()
		default:
		}

		if progressCallback != nil {
			progressCallback(i+1, len(bundles))
		}

		// Load bundle
		bundle, err := mgr.LoadBundleForDIDIndex(ctx, meta.BundleNumber)
		if err != nil {
			dim.logger.Printf("Warning: failed to load bundle %d: %v", meta.BundleNumber, err)
			continue
		}

		// Process each operation
		for pos, op := range bundle.Operations {
			identifier, err := extractDIDIdentifier(op.DID)
			if err != nil {
				continue
			}

			shardNum := dim.calculateShard(identifier)

			// Write entry: [24 bytes ID][2 bytes bundle][2 bytes pos][1 byte nullified]
			entry := make([]byte, 29)
			copy(entry[0:24], identifier)
			binary.LittleEndian.PutUint16(entry[24:26], uint16(meta.BundleNumber))
			binary.LittleEndian.PutUint16(entry[26:28], uint16(pos))

			// Store nullified flag
			if op.IsNullified() {
				entry[28] = 1
			} else {
				entry[28] = 0
			}

			if _, err := tempShards[shardNum].Write(entry); err != nil {
				dim.logger.Printf("Warning: failed to write to temp shard %02x: %v", shardNum, err)
			}
		}
	}

	// Close temp files
	for _, f := range tempShards {
		f.Close()
	}

	dim.logger.Printf("\n")
	dim.logger.Printf("Pass 2/2: Consolidating %d shards...", DID_SHARD_COUNT)

	// Consolidate shards
	totalDIDs := int64(0)
	for i := 0; i < DID_SHARD_COUNT; i++ {
		// Log every 32 shards
		if i%32 == 0 || i == DID_SHARD_COUNT-1 {
			dim.logger.Printf("  Consolidating shards: %d/%d (%.1f%%)",
				i+1, DID_SHARD_COUNT, float64(i+1)/float64(DID_SHARD_COUNT)*100)
		}

		count, err := dim.consolidateShard(uint8(i))
		if err != nil {
			return fmt.Errorf("failed to consolidate shard %02x: %w", i, err)
		}
		totalDIDs += count
	}

	dim.config.TotalDIDs = totalDIDs
	dim.config.LastBundle = bundles[len(bundles)-1].BundleNumber

	if err := dim.saveIndexConfig(); err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	dim.logger.Printf("✓ Index built: %d DIDs indexed", totalDIDs)

	return nil
}

// consolidateShard reads temp file, sorts, and writes final shard
func (dim *Manager) consolidateShard(shardNum uint8) (int64, error) {
	tempPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.tmp", shardNum))

	// Read all entries from temp file
	data, err := os.ReadFile(tempPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	if len(data) == 0 {
		os.Remove(tempPath)
		return 0, nil
	}

	// Parse entries (29 bytes each)
	entryCount := len(data) / 29
	if len(data)%29 != 0 {
		return 0, fmt.Errorf("corrupted temp shard: size not multiple of 29")
	}

	type tempEntry struct {
		identifier string
		bundle     uint16
		position   uint16
		nullified  bool
	}

	entries := make([]tempEntry, entryCount)
	for i := 0; i < entryCount; i++ {
		offset := i * 29
		entries[i] = tempEntry{
			identifier: string(data[offset : offset+24]),
			bundle:     binary.LittleEndian.Uint16(data[offset+24 : offset+26]),
			position:   binary.LittleEndian.Uint16(data[offset+26 : offset+28]),
			nullified:  data[offset+28] != 0,
		}
	}

	// Free the data slice
	data = nil

	// Sort by identifier
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].identifier < entries[j].identifier
	})

	// Group by DID
	builder := newShardBuilder()
	for _, entry := range entries {
		builder.add(entry.identifier, entry.bundle, entry.position, entry.nullified)
	}

	// Free entries
	entries = nil

	// Write final shard
	if err := dim.writeShard(shardNum, builder); err != nil {
		return 0, err
	}

	// Clean up temp file
	os.Remove(tempPath)

	return int64(len(builder.entries)), nil
}

// UpdateIndexForBundle adds operations from a new bundle (incremental + ATOMIC)
func (dim *Manager) UpdateIndexForBundle(ctx context.Context, bundle *BundleData) error {
	dim.indexMu.Lock()
	defer dim.indexMu.Unlock()

	// Group operations by shard
	shardOps := make(map[uint8]map[string][]OpLocation)

	for pos, op := range bundle.Operations {
		identifier, err := extractDIDIdentifier(op.DID)
		if err != nil {
			continue
		}

		shardNum := dim.calculateShard(identifier)

		if shardOps[shardNum] == nil {
			shardOps[shardNum] = make(map[string][]OpLocation)
		}

		shardOps[shardNum][identifier] = append(shardOps[shardNum][identifier], OpLocation{
			Bundle:    uint16(bundle.BundleNumber),
			Position:  uint16(pos),
			Nullified: op.IsNullified(),
		})
	}

	// PHASE 1: Write ALL shards to .tmp files FIRST
	tmpShards := make(map[uint8]string)
	var deltaCount int64

	for shardNum, newOps := range shardOps {
		tmpPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx.tmp", shardNum))

		addedCount, err := dim.updateShardToTemp(shardNum, newOps, tmpPath)
		if err != nil {
			dim.cleanupTempShards(tmpShards)
			return fmt.Errorf("failed to prepare shard %02x: %w", shardNum, err)
		}

		tmpShards[shardNum] = tmpPath
		deltaCount += addedCount
	}

	// PHASE 2: Atomically commit ALL shards
	for shardNum, tmpPath := range tmpShards {
		finalPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx", shardNum))

		if err := os.Rename(tmpPath, finalPath); err != nil {
			dim.logger.Printf("ERROR: Failed to commit shard %02x: %v", shardNum, err)
			return fmt.Errorf("failed to commit shard %02x: %w", shardNum, err)
		}

		// Invalidate cache
		dim.invalidateShard(shardNum)
	}

	// PHASE 3: Update config
	dim.config.TotalDIDs += deltaCount
	dim.config.LastBundle = bundle.BundleNumber

	return dim.saveIndexConfig()
}

// updateShardToTemp updates a shard and writes to temp file
func (dim *Manager) updateShardToTemp(shardNum uint8, newOps map[string][]OpLocation, tmpPath string) (int64, error) {
	shardPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx", shardNum))

	existingBuilder := newShardBuilder()

	// Read existing shard if it exists
	if data, err := os.ReadFile(shardPath); err == nil && len(data) > 32 {
		if err := dim.parseShardData(data, existingBuilder); err != nil {
			return 0, fmt.Errorf("failed to parse existing shard: %w", err)
		}
	}

	beforeCount := len(existingBuilder.entries)

	// Merge new operations
	for identifier, locations := range newOps {
		existingBuilder.entries[identifier] = append(existingBuilder.entries[identifier], locations...)
	}

	afterCount := len(existingBuilder.entries)
	deltaCount := int64(afterCount - beforeCount)

	// Write to TEMP file
	if err := dim.writeShardToPath(tmpPath, shardNum, existingBuilder); err != nil {
		return 0, err
	}

	return deltaCount, nil
}

// cleanupTempShards removes all temporary shard files
func (dim *Manager) cleanupTempShards(tmpShards map[uint8]string) {
	for shardNum, tmpPath := range tmpShards {
		if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
			dim.logger.Printf("Warning: failed to cleanup temp shard %02x: %v", shardNum, err)
		}
	}
}

// VerifyAndRepairIndex checks if index is consistent with bundles and repairs if needed
func (dim *Manager) VerifyAndRepairIndex(ctx context.Context, mgr BundleProvider) error {
	bundles := mgr.GetBundleIndex().GetBundles()
	if len(bundles) == 0 {
		return nil
	}

	lastBundleInRepo := bundles[len(bundles)-1].BundleNumber
	lastBundleInIndex := dim.config.LastBundle

	if lastBundleInIndex == lastBundleInRepo {
		return nil
	}

	if lastBundleInIndex > lastBundleInRepo {
		dim.logger.Printf("⚠️  Warning: Index claims bundle %d but only %d bundles exist",
			lastBundleInIndex, lastBundleInRepo)
		dim.logger.Printf("    Rebuilding index...")
		return dim.BuildIndexFromScratch(ctx, mgr, nil)
	}

	// Index is behind - update incrementally
	dim.logger.Printf("Index is behind: has bundle %d, need %d",
		lastBundleInIndex, lastBundleInRepo)
	dim.logger.Printf("Updating index for %d missing bundles...",
		lastBundleInRepo-lastBundleInIndex)

	for bundleNum := lastBundleInIndex + 1; bundleNum <= lastBundleInRepo; bundleNum++ {
		bundle, err := mgr.LoadBundleForDIDIndex(ctx, bundleNum)
		if err != nil {
			return fmt.Errorf("failed to load bundle %d: %w", bundleNum, err)
		}

		if err := dim.UpdateIndexForBundle(ctx, bundle); err != nil {
			return fmt.Errorf("failed to update index for bundle %d: %w", bundleNum, err)
		}

		if bundleNum%100 == 0 {
			dim.logger.Printf("  Updated through bundle %d...", bundleNum)
		}
	}

	dim.logger.Printf("✓ Index repaired: now at bundle %d", lastBundleInRepo)
	return nil
}
