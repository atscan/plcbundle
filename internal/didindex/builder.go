package didindex

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

// newShardBuilder creates a new shard builder
func newShardBuilder() *ShardBuilder {
	return &ShardBuilder{
		entries: make(map[string][]OpLocation),
	}
}

// add adds a location to the shard
func (sb *ShardBuilder) add(identifier string, loc OpLocation) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	sb.entries[identifier] = append(sb.entries[identifier], loc)
}

// updateAndSaveConfig updates config with new values and saves atomically
func (dim *Manager) updateAndSaveConfig(totalDIDs int64, lastBundle int) error {
	dim.config.TotalDIDs = totalDIDs
	dim.config.LastBundle = lastBundle
	dim.config.Version = DIDINDEX_VERSION
	dim.config.Format = "binary_v4"
	dim.config.UpdatedAt = time.Now().UTC()

	return dim.saveIndexConfig()
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

	if err := os.MkdirAll(dim.shardDir, 0755); err != nil {
		return fmt.Errorf("failed to create shard directory: %w", err)
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

			// Write entry: [24 bytes ID][4 bytes packed OpLocation]
			entry := make([]byte, 28)
			copy(entry[0:24], identifier)

			// Create packed OpLocation (includes nullified bit)
			loc := NewOpLocation(uint16(meta.BundleNumber), uint16(pos), op.IsNullified())
			binary.LittleEndian.PutUint32(entry[24:28], uint32(loc))

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

	if err := dim.updateAndSaveConfig(totalDIDs, bundles[len(bundles)-1].BundleNumber); err != nil {
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

	// Parse entries (28 bytes each)
	entryCount := len(data) / 28
	if len(data)%28 != 0 {
		return 0, fmt.Errorf("corrupted temp shard: size not multiple of 28")
	}

	type tempEntry struct {
		identifier string
		location   OpLocation
	}

	entries := make([]tempEntry, entryCount)
	for i := 0; i < entryCount; i++ {
		offset := i * 28
		entries[i] = tempEntry{
			identifier: string(data[offset : offset+24]),
			location:   OpLocation(binary.LittleEndian.Uint32(data[offset+24 : offset+28])),
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
		builder.add(entry.identifier, entry.location)
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

// UpdateIndexForBundle adds operations from a new bundle (incremental + ATOMIC + PARALLEL)
func (dim *Manager) UpdateIndexForBundle(ctx context.Context, bundle *BundleData) error {
	dim.indexMu.Lock()
	defer dim.indexMu.Unlock()

	totalStart := time.Now()

	// STEP 1: Group operations by shard
	groupStart := time.Now()
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

		loc := NewOpLocation(uint16(bundle.BundleNumber), uint16(pos), op.IsNullified())
		shardOps[shardNum][identifier] = append(shardOps[shardNum][identifier], loc)
	}

	groupDuration := time.Since(groupStart)
	if dim.verbose {
		dim.logger.Printf("  [DID Index] Grouped operations into %d shards in %s",
			len(shardOps), groupDuration)
	}

	// STEP 2: Write ALL shards to .tmp files FIRST (PARALLEL)
	writeStart := time.Now()

	tmpShards := make(map[uint8]string)
	var tmpShardsMu sync.Mutex
	var deltaCount int64
	var deltaCountMu sync.Mutex

	// Error handling
	errChan := make(chan error, len(shardOps))

	// Worker pool
	workers := runtime.NumCPU()
	if workers > len(shardOps) {
		workers = len(shardOps)
	}
	if workers < 1 {
		workers = 1
	}

	semaphore := make(chan struct{}, workers)
	var wg sync.WaitGroup

	if dim.verbose {
		dim.logger.Printf("  [DID Index] Updating %d shards in parallel (%d workers)...",
			len(shardOps), workers)
	}

	// Process each shard in parallel
	for shardNum, newOps := range shardOps {
		wg.Add(1)
		go func(sNum uint8, ops map[string][]OpLocation) {
			defer wg.Done()

			// Acquire semaphore (limit concurrency)
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			shardStart := time.Now()
			tmpPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx.tmp", sNum))

			addedCount, err := dim.updateShardToTemp(sNum, ops, tmpPath)
			if err != nil {
				errChan <- fmt.Errorf("shard %02x: %w", sNum, err)
				return
			}

			shardDuration := time.Since(shardStart)

			// Update shared state
			tmpShardsMu.Lock()
			tmpShards[sNum] = tmpPath
			tmpShardsMu.Unlock()

			deltaCountMu.Lock()
			deltaCount += addedCount
			deltaCountMu.Unlock()

			// Debug log for each shard
			if dim.verbose {
				dim.logger.Printf("    Shard %02x: +%d DIDs in %s (%d ops)",
					sNum, addedCount, shardDuration, len(ops))
			}
		}(shardNum, newOps)
	}

	// Wait for all workers
	wg.Wait()
	close(errChan)

	writeDuration := time.Since(writeStart)
	if dim.verbose {
		dim.logger.Printf("  [DID Index] Wrote %d temp files in %s (%.1f shards/sec)",
			len(tmpShards), writeDuration, float64(len(tmpShards))/writeDuration.Seconds())
	}

	// Check for errors
	if err := <-errChan; err != nil {
		dim.cleanupTempShards(tmpShards)
		return err
	}

	// STEP 3: Atomically commit ALL shards
	commitStart := time.Now()

	for shardNum, tmpPath := range tmpShards {
		finalPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx", shardNum))

		if err := os.Rename(tmpPath, finalPath); err != nil {
			dim.logger.Printf("ERROR: Failed to commit shard %02x: %v", shardNum, err)
			return fmt.Errorf("failed to commit shard %02x: %w", shardNum, err)
		}

		// Invalidate cache
		dim.invalidateShard(shardNum)
	}

	commitDuration := time.Since(commitStart)

	// STEP 4: Update config
	configStart := time.Now()

	newTotal := dim.config.TotalDIDs + deltaCount
	if err := dim.updateAndSaveConfig(newTotal, bundle.BundleNumber); err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	configDuration := time.Since(configStart)
	totalDuration := time.Since(totalStart)

	// Summary log
	if dim.verbose {
		dim.logger.Printf("  [DID Index] ✓ Bundle %06d indexed: +%d DIDs, %d shards updated in %s",
			bundle.BundleNumber, deltaCount, len(tmpShards), totalDuration)
	}

	if dim.verbose {
		dim.logger.Printf("    Breakdown: group=%s write=%s commit=%s config=%s",
			groupDuration, writeDuration, commitDuration, configDuration)
		dim.logger.Printf("    Throughput: %.0f ops/sec",
			float64(len(bundle.Operations))/totalDuration.Seconds())
	}

	return nil
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
