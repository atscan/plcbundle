package bundle

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	BUILD_BATCH_SIZE = 100 // Process 100 bundles at a time (not used in streaming approach)
)

// ShardBuilder accumulates DID positions for a shard
type ShardBuilder struct {
	entries map[string][]OpLocation
	mu      sync.Mutex
}

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
func (dim *DIDIndexManager) BuildIndexFromScratch(ctx context.Context, mgr *Manager, progressCallback func(current, total int)) error {
	dim.logger.Printf("Building DID index from scratch (memory-efficient mode)...")

	bundles := mgr.index.GetBundles()
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
		bundle, err := mgr.LoadBundle(ctx, meta.BundleNumber)
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
func (dim *DIDIndexManager) consolidateShard(shardNum uint8) (int64, error) {
	tempPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.tmp", shardNum))

	// Read all entries from temp file
	data, err := os.ReadFile(tempPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // No entries for this shard
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
func (dim *DIDIndexManager) UpdateIndexForBundle(ctx context.Context, bundle *Bundle) error {
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

	// ✨ PHASE 1: Write ALL shards to .tmp files FIRST (no commits yet)
	tmpShards := make(map[uint8]string)
	var deltaCount int64 // Track new DIDs added

	for shardNum, newOps := range shardOps {
		tmpPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx.tmp", shardNum))

		// Update shard to temp file
		addedCount, err := dim.updateShardToTemp(shardNum, newOps, tmpPath)
		if err != nil {
			// ❌ Cleanup all temp files on ANY error
			dim.cleanupTempShards(tmpShards)
			return fmt.Errorf("failed to prepare shard %02x: %w", shardNum, err)
		}

		tmpShards[shardNum] = tmpPath
		deltaCount += addedCount
	}

	// ✨ PHASE 2: ALL temp files ready - atomically commit ALL shards
	for shardNum, tmpPath := range tmpShards {
		finalPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx", shardNum))

		if err := os.Rename(tmpPath, finalPath); err != nil {
			// This is bad - some shards committed, some didn't
			// Log error but don't rollback (rename is usually atomic)
			dim.logger.Printf("ERROR: Failed to commit shard %02x: %v", shardNum, err)
			return fmt.Errorf("failed to commit shard %02x: %w", shardNum, err)
		}

		// Invalidate cache for updated shard
		dim.invalidateShard(shardNum)
	}

	// ✨ PHASE 3: Update config ONLY after all shards committed
	dim.config.TotalDIDs += deltaCount
	dim.config.LastBundle = bundle.BundleNumber

	return dim.saveIndexConfig()
}

// parseShardData parses binary shard data into builder (v2 format only)
func (dim *DIDIndexManager) parseShardData(data []byte, builder *ShardBuilder) error {
	if len(data) < 32 {
		return nil
	}

	entryCount := binary.LittleEndian.Uint32(data[9:13])

	// Skip offset table, start at first entry
	offset := 32 + (int(entryCount) * 4)

	for i := 0; i < int(entryCount); i++ {
		if offset+DID_IDENTIFIER_LEN+2 > len(data) {
			break
		}

		// Read identifier
		identifier := string(data[offset : offset+DID_IDENTIFIER_LEN])
		offset += DID_IDENTIFIER_LEN

		// Read location count
		locCount := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2

		// Read locations
		locations := make([]OpLocation, locCount)
		for j := 0; j < int(locCount); j++ {
			if offset+5 > len(data) {
				break
			}

			locations[j] = OpLocation{
				Bundle:    binary.LittleEndian.Uint16(data[offset : offset+2]),
				Position:  binary.LittleEndian.Uint16(data[offset+2 : offset+4]),
				Nullified: data[offset+4] != 0,
			}
			offset += 5
		}

		builder.entries[identifier] = locations
	}

	return nil
}

// writeShard writes a shard to disk in binary format with offset table
func (dim *DIDIndexManager) writeShard(shardNum uint8, builder *ShardBuilder) error {
	// Write to temp file first
	shardPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx", shardNum))
	tempPath := shardPath + ".tmp"

	if err := dim.writeShardToPath(tempPath, shardNum, builder); err != nil {
		return err
	}

	// Atomic rename
	if err := os.Rename(tempPath, shardPath); err != nil {
		os.Remove(tempPath)
		return err
	}

	// Invalidate cache for this shard
	dim.invalidateShard(shardNum)

	return nil
}

// invalidateShard removes a shard from cache
func (dim *DIDIndexManager) invalidateShard(shardNum uint8) {
	dim.cacheMu.Lock()
	defer dim.cacheMu.Unlock()

	if cached, exists := dim.shardCache[shardNum]; exists {
		dim.unmapShard(cached)
		delete(dim.shardCache, shardNum)
	}
}

// updateShardToTemp updates a shard and writes to temp file
// Returns: number of NEW DIDs added (delta count)
func (dim *DIDIndexManager) updateShardToTemp(shardNum uint8, newOps map[string][]OpLocation, tmpPath string) (int64, error) {
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

	// Write to TEMP file (not final location yet)
	if err := dim.writeShardToPath(tmpPath, shardNum, existingBuilder); err != nil {
		return 0, err
	}

	return deltaCount, nil
}

// writeShardToPath writes shard to a specific path (refactored from writeShard)
func (dim *DIDIndexManager) writeShardToPath(path string, shardNum uint8, builder *ShardBuilder) error {
	// Sort identifiers for binary search
	identifiers := make([]string, 0, len(builder.entries))
	for id := range builder.entries {
		identifiers = append(identifiers, id)
	}
	sort.Strings(identifiers)

	// Calculate entry offsets
	offsetTable := make([]uint32, len(identifiers))
	dataStartOffset := 32 + (len(identifiers) * 4)

	currentOffset := dataStartOffset
	for i, id := range identifiers {
		offsetTable[i] = uint32(currentOffset)
		locations := builder.entries[id]
		entrySize := DID_IDENTIFIER_LEN + 2 + (len(locations) * 5)
		currentOffset += entrySize
	}

	totalSize := currentOffset

	// Allocate buffer
	buf := make([]byte, totalSize)

	// Write header (32 bytes)
	copy(buf[0:4], DIDINDEX_MAGIC)
	binary.LittleEndian.PutUint32(buf[4:8], DIDINDEX_VERSION)
	buf[8] = shardNum
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(identifiers)))

	// Write offset table
	offsetTableStart := 32
	for i, offset := range offsetTable {
		pos := offsetTableStart + (i * 4)
		binary.LittleEndian.PutUint32(buf[pos:pos+4], offset)
	}

	// Write entries
	for i, identifier := range identifiers {
		offset := int(offsetTable[i])
		locations := builder.entries[identifier]

		copy(buf[offset:offset+DID_IDENTIFIER_LEN], identifier)
		offset += DID_IDENTIFIER_LEN

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(locations)))
		offset += 2

		for _, loc := range locations {
			binary.LittleEndian.PutUint16(buf[offset:offset+2], loc.Bundle)
			binary.LittleEndian.PutUint16(buf[offset+2:offset+4], loc.Position)

			if loc.Nullified {
				buf[offset+4] = 1
			} else {
				buf[offset+4] = 0
			}

			offset += 5
		}
	}

	// ✨ Write directly to specified path (no rename needed)
	return os.WriteFile(path, buf, 0644)
}

// cleanupTempShards removes all temporary shard files
func (dim *DIDIndexManager) cleanupTempShards(tmpShards map[uint8]string) {
	for shardNum, tmpPath := range tmpShards {
		if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
			dim.logger.Printf("Warning: failed to cleanup temp shard %02x: %v", shardNum, err)
		}
	}
}

// VerifyAndRepairIndex checks if index is consistent with bundles and repairs if needed
func (dim *DIDIndexManager) VerifyAndRepairIndex(ctx context.Context, mgr *Manager) error {
	bundles := mgr.index.GetBundles()
	if len(bundles) == 0 {
		return nil
	}

	lastBundleInRepo := bundles[len(bundles)-1].BundleNumber
	lastBundleInIndex := dim.config.LastBundle

	if lastBundleInIndex == lastBundleInRepo {
		// Index claims to be up to date
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
		bundle, err := mgr.LoadBundle(ctx, bundleNum)
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
