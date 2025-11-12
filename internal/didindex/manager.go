package didindex

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/goccy/go-json"
	"golang.org/x/sys/unix"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

// NewManager creates a new DID index manager
func NewManager(baseDir string, logger Logger) *Manager {
	indexDir := filepath.Join(baseDir, DID_INDEX_DIR)
	shardDir := filepath.Join(indexDir, DID_INDEX_SHARDS)
	configPath := filepath.Join(indexDir, DID_INDEX_CONFIG)

	// Load or create config
	config, _ := loadIndexConfig(configPath)
	if config == nil {
		config = &Config{
			Version:    DIDINDEX_VERSION, // Will be 4
			Format:     "binary_v4",      // Update format name
			ShardCount: DID_SHARD_COUNT,
			UpdatedAt:  time.Now().UTC(),
		}
	} else if config.Version < DIDINDEX_VERSION {
		// Auto-trigger rebuild on version mismatch
		logger.Printf("DID index version outdated (v%d, need v%d) - rebuild required",
			config.Version, DIDINDEX_VERSION)
	}

	return &Manager{
		baseDir:           baseDir,
		indexDir:          indexDir,
		shardDir:          shardDir,
		configPath:        configPath,
		maxCache:          5,
		evictionThreshold: 5,
		config:            config,
		logger:            logger,
		recentLookupSize:  1000, // Track last 100 lookups
		recentLookups:     make([]int64, 1000),
	}
}

// Close unmaps all shards and cleans up
func (dim *Manager) Close() error {
	// Mark all shards for eviction
	var shards []*mmapShard

	dim.shardCache.Range(func(key, value interface{}) bool {
		shard := value.(*mmapShard)
		shards = append(shards, shard)
		dim.shardCache.Delete(key)
		return true
	})

	// Wait for refcounts to drop to 0
	for _, shard := range shards {
		for atomic.LoadInt64(&shard.refCount) > 0 {
			time.Sleep(1 * time.Millisecond)
		}
		dim.unmapShard(shard)
	}

	return nil
}

func (dim *Manager) SetVerbose(verbose bool) {
	dim.verbose = verbose
}

// GetDIDLocations returns all bundle+position locations for a DID (with timing)
func (dim *Manager) GetDIDLocations(did string) ([]OpLocation, error) {
	// Start timing
	lookupStart := time.Now()
	defer func() {
		dim.recordLookupTime(time.Since(lookupStart))
	}()

	identifier, err := extractDIDIdentifier(did)
	if err != nil {
		return nil, err
	}

	shardNum := dim.calculateShard(identifier)
	if dim.verbose {
		dim.logger.Printf("DEBUG: DID %s -> identifier '%s' -> shard %02x", did, identifier, shardNum)
	}

	shard, err := dim.loadShard(shardNum)
	if err != nil {
		if dim.verbose {
			dim.logger.Printf("DEBUG: Failed to load shard: %v", err)
		}
		return nil, fmt.Errorf("failed to load shard %02x: %w", shardNum, err)
	}

	defer dim.releaseShard(shard)

	if shard.data == nil {
		if dim.verbose {
			dim.logger.Printf("DEBUG: Shard %02x has no data (empty shard)", shardNum)
		}
		return nil, nil
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Shard %02x loaded, size: %d bytes", shardNum, len(shard.data))
	}

	locations := dim.searchShard(shard, identifier)

	if dim.verbose {
		dim.logger.Printf("DEBUG: Binary search found %d locations", len(locations))
		if len(locations) > 0 {
			dim.logger.Printf("DEBUG: Locations: %v", locations)
		}
	}

	return locations, nil
}

// calculateShard determines which shard a DID belongs to
func (dim *Manager) calculateShard(identifier string) uint8 {
	h := fnv.New32a()
	h.Write([]byte(identifier))
	hash := h.Sum32()
	return uint8(hash % DID_SHARD_COUNT)
}

// loadShard loads a shard from cache or disk (with madvise optimization)
func (dim *Manager) loadShard(shardNum uint8) (*mmapShard, error) {
	// Fast path: cache hit
	if val, ok := dim.shardCache.Load(shardNum); ok {
		shard := val.(*mmapShard)

		// Increment refcount BEFORE returning
		atomic.AddInt64(&shard.refCount, 1)
		atomic.StoreInt64(&shard.lastUsed, time.Now().Unix())
		atomic.AddInt64(&shard.accessCount, 1)
		atomic.AddInt64(&dim.cacheHits, 1)

		return shard, nil
	}
	atomic.AddInt64(&dim.cacheMisses, 1)

	// Cache miss - load from disk
	shardPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx", shardNum))

	if _, err := os.Stat(shardPath); os.IsNotExist(err) {
		// Empty shard - no refcount needed
		return &mmapShard{
			shardNum: shardNum,
			data:     nil,
			lastUsed: time.Now().Unix(),
			refCount: 0, // Not in cache
		}, nil
	}

	file, err := os.Open(shardPath)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		file.Close()
		return &mmapShard{
			shardNum: shardNum,
			data:     nil,
			lastUsed: time.Now().Unix(),
			refCount: 0,
		}, nil
	}

	// Memory-map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, int(info.Size()),
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	if err := dim.applyMadviseHints(data, info.Size()); err != nil {
		if dim.verbose {
			dim.logger.Printf("DEBUG: madvise failed (non-fatal): %v", err)
		}
	}

	shard := &mmapShard{
		shardNum:    shardNum,
		data:        data,
		file:        file,
		lastUsed:    time.Now().Unix(),
		accessCount: 1,
		refCount:    1,
	}

	// Try to store
	actual, loaded := dim.shardCache.LoadOrStore(shardNum, shard)

	if loaded {
		// Someone else loaded it - cleanup ours
		dim.unmapShard(shard)

		actualShard := actual.(*mmapShard)
		atomic.AddInt64(&actualShard.refCount, 1) // Increment their refcount
		atomic.StoreInt64(&actualShard.lastUsed, time.Now().Unix())
		atomic.AddInt64(&actualShard.accessCount, 1)
		return actualShard, nil
	}

	// We stored it - maybe evict
	go dim.evictIfNeeded() // Run async to avoid blocking

	return shard, nil
}

// applyMadviseHints applies OS-level memory hints for optimal performance
func (dim *Manager) applyMadviseHints(data []byte, fileSize int64) error {
	const headerPrefetchSize = 16 * 1024 // 16 KB for header + prefix + start of offset table

	// 1. Prefetch critical header section (prefix index + offset table start)
	if len(data) >= headerPrefetchSize {
		if err := unix.Madvise(data[:headerPrefetchSize], unix.MADV_WILLNEED); err != nil {
			if dim.verbose {
				dim.logger.Printf("DEBUG: [madvise] Header prefetch failed: %v", err)
			}
		} else if dim.verbose {
			dim.logger.Printf("DEBUG: [madvise] Prefetching header (%d KB) + marking rest as RANDOM",
				headerPrefetchSize/1024)
		}
	}

	// 2. Mark rest as random access (tells OS not to do read-ahead)
	if err := unix.Madvise(data, unix.MADV_RANDOM); err != nil {
		return err
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: [madvise] Shard size: %.1f MB → RANDOM access pattern",
			float64(fileSize)/(1024*1024))
	}

	return nil
}

// searchShard performs optimized binary search using prefix index
func (dim *Manager) searchShard(shard *mmapShard, identifier string) []OpLocation {
	if len(shard.data) < 1056 {
		return nil
	}

	data := shard.data

	// Read header
	if string(data[0:4]) != DIDINDEX_MAGIC {
		dim.logger.Printf("Warning: invalid shard magic")
		return nil
	}

	version := binary.LittleEndian.Uint32(data[4:8])
	entryCount := binary.LittleEndian.Uint32(data[9:13])

	if entryCount == 0 {
		return nil
	}

	// Determine search range using prefix index
	left, right := 0, int(entryCount)

	// Use prefix index to narrow range (only for v3+)
	if version >= 3 && len(identifier) > 0 {
		prefixByte := identifier[0]
		prefixIndexPos := 32 + (int(prefixByte) * 4)

		if prefixIndexPos+4 <= len(data) {
			startIdx := binary.LittleEndian.Uint32(data[prefixIndexPos : prefixIndexPos+4])

			if startIdx != 0xFFFFFFFF {
				left = int(startIdx)

				// Find end of this prefix range
				for nextPrefix := int(prefixByte) + 1; nextPrefix < 256; nextPrefix++ {
					nextPos := 32 + (nextPrefix * 4)
					if nextPos+4 > len(data) {
						break
					}
					nextIdx := binary.LittleEndian.Uint32(data[nextPos : nextPos+4])
					if nextIdx != 0xFFFFFFFF {
						right = int(nextIdx)
						break
					}
				}

				if dim.verbose {
					dim.logger.Printf("DEBUG: Prefix index narrowed search: %d entries → %d entries (%.1f%% reduction)",
						entryCount, right-left, (1.0-float64(right-left)/float64(entryCount))*100)
				}
			} else {
				// No entries with this prefix
				if dim.verbose {
					dim.logger.Printf("DEBUG: Prefix index: no entries with prefix 0x%02x", prefixByte)
				}
				return nil
			}
		}
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Binary search range: [%d, %d) of %d entries", left, right, entryCount)
	}

	// Binary search within narrowed range
	attempts := 0
	offsetTableStart := 1056 // After header + prefix index

	for left < right {
		attempts++
		mid := (left + right) / 2

		// Get entry offset from offset table
		offsetPos := offsetTableStart + (mid * 4)
		if offsetPos+4 > len(data) {
			if dim.verbose {
				dim.logger.Printf("DEBUG: Offset position out of bounds")
			}
			return nil
		}

		entryOffset := int(binary.LittleEndian.Uint32(data[offsetPos : offsetPos+4]))

		if entryOffset+DID_IDENTIFIER_LEN > len(data) {
			if dim.verbose {
				dim.logger.Printf("DEBUG: Entry offset out of bounds: %d + %d > %d",
					entryOffset, DID_IDENTIFIER_LEN, len(data))
			}
			return nil
		}

		entryID := string(data[entryOffset : entryOffset+DID_IDENTIFIER_LEN])

		if dim.verbose && attempts <= 5 {
			dim.logger.Printf("DEBUG: Attempt %d: mid=%d, comparing '%s' vs '%s'",
				attempts, mid, identifier, entryID)
		}

		// Compare
		if identifier == entryID {
			if dim.verbose {
				dim.logger.Printf("DEBUG: FOUND at mid=%d after %d attempts (vs ~%d without prefix index)",
					mid, attempts, logBase2(int(entryCount)))
			}
			return dim.readLocations(data, entryOffset)
		} else if identifier < entryID {
			right = mid
		} else {
			left = mid + 1
		}
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: NOT FOUND after %d attempts", attempts)
	}

	return nil
}

// Helper function
func logBase2(n int) int {
	if n <= 0 {
		return 0
	}
	count := 0
	for n > 1 {
		n >>= 1
		count++
	}
	return count
}

// getEntryOffset reads entry offset from offset table - O(1) lookup
func (dim *Manager) getEntryOffset(data []byte, entryIndex int) int {
	if len(data) < 1056 {
		return -1
	}

	entryCount := binary.LittleEndian.Uint32(data[9:13])
	if entryIndex < 0 || entryIndex >= int(entryCount) {
		return -1
	}

	// Offset table starts at 1056 (after header + prefix index)
	offsetTableStart := 1056
	offsetPos := offsetTableStart + (entryIndex * 4)

	if offsetPos+4 > len(data) {
		return -1
	}

	offset := int(binary.LittleEndian.Uint32(data[offsetPos : offsetPos+4]))

	if offset < 0 || offset >= len(data) {
		return -1
	}

	return offset
}

// readLocations reads location data at given offset
func (dim *Manager) readLocations(data []byte, offset int) []OpLocation {
	// Skip identifier
	offset += DID_IDENTIFIER_LEN

	// Read count
	if offset+2 > len(data) {
		return nil
	}
	count := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Read locations
	locations := make([]OpLocation, count)
	for i := 0; i < int(count); i++ {
		if offset+4 > len(data) {
			return locations[:i]
		}

		// Read packed uint32
		packed := binary.LittleEndian.Uint32(data[offset : offset+4])
		locations[i] = OpLocation(packed)

		offset += 4
	}

	return locations
}

// unmapShard unmaps and closes a shard
func (dim *Manager) unmapShard(shard *mmapShard) {
	if shard.data != nil {
		unix.Madvise(shard.data, unix.MADV_DONTNEED)

		syscall.Munmap(shard.data)
	}
	if shard.file != nil {
		if f, ok := shard.file.(*os.File); ok {
			f.Close()
		}
	}
}

// GetStats returns index statistics (updated)
func (dim *Manager) GetStats() map[string]interface{} {
	cachedShards := make([]int, 0)

	dim.shardCache.Range(func(key, value interface{}) bool {
		cachedShards = append(cachedShards, int(key.(uint8)))
		return true
	})

	sort.Ints(cachedShards)

	// Calculate cache hit rate
	hits := atomic.LoadInt64(&dim.cacheHits)
	misses := atomic.LoadInt64(&dim.cacheMisses)
	total := hits + misses

	cacheHitRate := 0.0
	if total > 0 {
		cacheHitRate = float64(hits) / float64(total)
	}

	baseStats := map[string]interface{}{
		"total_dids":     dim.config.TotalDIDs,
		"last_bundle":    dim.config.LastBundle,
		"shard_count":    dim.config.ShardCount,
		"cached_shards":  len(cachedShards),
		"cache_limit":    dim.maxCache,
		"cache_order":    cachedShards,
		"updated_at":     dim.config.UpdatedAt,
		"cache_hits":     hits,
		"cache_misses":   misses,
		"cache_hit_rate": cacheHitRate,
		"total_lookups":  total,
	}

	// Merge with performance stats
	perfStats := dim.calculateLookupStats()
	for k, v := range perfStats {
		baseStats[k] = v
	}

	return baseStats
}

// Exists checks if index exists
func (dim *Manager) Exists() bool {
	_, err := os.Stat(dim.configPath)
	return err == nil
}

// TrimCache trims cache to keep only most recent shard
func (dim *Manager) TrimCache() {
	// Count current size
	size := 0
	dim.shardCache.Range(func(k, v interface{}) bool {
		size++
		return true
	})

	if size <= 1 {
		return
	}

	// Find most recent shard
	var newestTime int64
	var keepNum uint8

	dim.shardCache.Range(func(key, value interface{}) bool {
		shard := value.(*mmapShard)
		lastUsed := atomic.LoadInt64(&shard.lastUsed)
		if lastUsed > newestTime {
			newestTime = lastUsed
			keepNum = key.(uint8)
		}
		return true
	})

	// Evict all except newest
	dim.shardCache.Range(func(key, value interface{}) bool {
		num := key.(uint8)
		if num != keepNum {
			if val, ok := dim.shardCache.LoadAndDelete(key); ok {
				shard := val.(*mmapShard)
				dim.unmapShard(shard)
			}
		}
		return true
	})
}

// GetConfig returns the index configuration
func (dim *Manager) GetConfig() *Config {
	return dim.config
}

// DebugShard shows shard debugging information
func (dim *Manager) DebugShard(shardNum uint8) error {
	shardPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx", shardNum))
	data, err := os.ReadFile(shardPath)
	if err != nil {
		return err
	}

	fmt.Printf("Shard %02x debug:\n", shardNum)
	fmt.Printf("  File size: %d bytes\n", len(data))
	fmt.Printf("  Magic: %s\n", string(data[0:4]))
	fmt.Printf("  Version: %d\n", binary.LittleEndian.Uint32(data[4:8]))
	fmt.Printf("  Shard num: %d\n", data[8])

	entryCount := binary.LittleEndian.Uint32(data[9:13])
	fmt.Printf("  Entry count: %d\n", entryCount)

	// Show offset table info
	offsetTableSize := int(entryCount) * 4
	dataStartOffset := 32 + offsetTableSize
	fmt.Printf("  Offset table size: %d bytes\n", offsetTableSize)
	fmt.Printf("  Data starts at: %d\n", dataStartOffset)

	// Show first few entries
	fmt.Printf("\n  First 5 entries:\n")

	for i := 0; i < 5 && i < int(entryCount); i++ {
		offset := dim.getEntryOffset(data, i)
		if offset < 0 || offset+DID_IDENTIFIER_LEN+2 > len(data) {
			break
		}

		identifier := string(data[offset : offset+DID_IDENTIFIER_LEN])
		locCount := binary.LittleEndian.Uint16(data[offset+DID_IDENTIFIER_LEN : offset+DID_IDENTIFIER_LEN+2])

		fmt.Printf("    %d. '%s' (%d locations) @ offset %d\n", i+1, identifier, locCount, offset)
	}

	return nil
}

func (dim *Manager) invalidateShard(shardNum uint8) {
	if val, ok := dim.shardCache.LoadAndDelete(shardNum); ok {
		shard := val.(*mmapShard)

		for atomic.LoadInt64(&shard.refCount) > 0 {
			time.Sleep(1 * time.Millisecond)
		}

		dim.unmapShard(shard)
	}
}

// writeShard writes a shard to disk in binary format with offset table
func (dim *Manager) writeShard(shardNum uint8, builder *ShardBuilder) error {
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

// writeShardToPath writes shard to a specific path with prefix index
func (dim *Manager) writeShardToPath(path string, shardNum uint8, builder *ShardBuilder) error {
	// Sort identifiers for binary search
	identifiers := make([]string, 0, len(builder.entries))
	for id := range builder.entries {
		identifiers = append(identifiers, id)
	}
	sort.Strings(identifiers)

	if len(identifiers) == 0 {
		// Write empty shard
		return os.WriteFile(path, make([]byte, 0), 0644)
	}

	// Build prefix index: map first byte → first entry index with that prefix
	prefixIndex := make([]uint32, 256)
	for i := range prefixIndex {
		prefixIndex[i] = 0xFFFFFFFF // Marker for "no entries"
	}

	for i, identifier := range identifiers {
		if len(identifier) == 0 {
			continue
		}
		prefixByte := identifier[0]

		// Set to first occurrence only
		if prefixIndex[prefixByte] == 0xFFFFFFFF {
			prefixIndex[prefixByte] = uint32(i)
		}
	}

	// Calculate entry offsets
	offsetTableStart := 1056 // After header (32) + prefix index (1024)
	dataStartOffset := offsetTableStart + (len(identifiers) * 4)

	offsetTable := make([]uint32, len(identifiers))
	currentOffset := dataStartOffset

	for i, id := range identifiers {
		offsetTable[i] = uint32(currentOffset)
		locations := builder.entries[id]
		entrySize := DID_IDENTIFIER_LEN + 2 + (len(locations) * 4)
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
	// bytes 13-31: reserved (zeros)

	// Write prefix index (32-1055: 256 × 4 bytes)
	for i, entryIdx := range prefixIndex {
		pos := 32 + (i * 4)
		binary.LittleEndian.PutUint32(buf[pos:pos+4], entryIdx)
	}

	// Write offset table (1056+)
	for i, offset := range offsetTable {
		pos := offsetTableStart + (i * 4)
		binary.LittleEndian.PutUint32(buf[pos:pos+4], offset)
	}

	// Write entries (same as before)
	for i, identifier := range identifiers {
		offset := int(offsetTable[i])
		locations := builder.entries[identifier]

		copy(buf[offset:offset+DID_IDENTIFIER_LEN], identifier)
		offset += DID_IDENTIFIER_LEN

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(locations)))
		offset += 2

		for _, loc := range locations {
			// Write packed uint32 (global position + nullified bit)
			binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(loc))
			offset += 4
		}
	}

	return os.WriteFile(path, buf, 0644)
}

// parseShardData parses binary shard data into builder (supports v2 and v3)
func (dim *Manager) parseShardData(data []byte, builder *ShardBuilder) error {
	if len(data) < 32 {
		return nil
	}

	entryCount := binary.LittleEndian.Uint32(data[9:13])

	offsetTableStart := 1056

	// Start reading entries after offset table
	offset := offsetTableStart + (int(entryCount) * 4)

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

		// Check version to determine format
		version := binary.LittleEndian.Uint32(data[4:8])

		for j := 0; j < int(locCount); j++ {
			if version >= 4 {
				// New format: 4-byte packed uint32
				if offset+4 > len(data) {
					break
				}
				packed := binary.LittleEndian.Uint32(data[offset : offset+4])
				locations[j] = OpLocation(packed)
				offset += 4
			} else {
				// Old format: 5-byte separate fields (for migration)
				if offset+5 > len(data) {
					break
				}
				bundle := binary.LittleEndian.Uint16(data[offset : offset+2])
				position := binary.LittleEndian.Uint16(data[offset+2 : offset+4])
				nullified := data[offset+4] != 0

				// Convert to new format
				locations[j] = NewOpLocation(bundle, position, nullified)
				offset += 5
			}
		}

		builder.entries[identifier] = locations
	}

	return nil
}

// saveIndexConfig saves index configuration
func (dim *Manager) saveIndexConfig() error {
	dim.config.UpdatedAt = time.Now().UTC()

	data, err := json.MarshalIndent(dim.config, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(dim.configPath, data, 0644)
}

// extractDIDIdentifier extracts the 24-char identifier from full DID
func extractDIDIdentifier(did string) (string, error) {
	if err := plcclient.ValidateDIDFormat(did); err != nil {
		return "", err
	}

	// Remove "did:plc:" prefix
	identifier := did[8:]

	if len(identifier) != DID_IDENTIFIER_LEN {
		return "", fmt.Errorf("invalid identifier length: %d", len(identifier))
	}

	return identifier, nil
}

// loadIndexConfig loads index configuration
func loadIndexConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

func (dim *Manager) evictIfNeeded() {
	size := 0
	dim.shardCache.Range(func(_, _ interface{}) bool {
		size++
		return true
	})

	if size <= dim.evictionThreshold {
		return
	}

	type entry struct {
		num      uint8
		lastUsed int64
		refCount int64
	}

	var entries []entry

	dim.shardCache.Range(func(key, value interface{}) bool {
		shard := value.(*mmapShard)
		entries = append(entries, entry{
			num:      key.(uint8),
			lastUsed: atomic.LoadInt64(&shard.lastUsed),
			refCount: atomic.LoadInt64(&shard.refCount),
		})
		return true
	})

	// Sort by lastUsed (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastUsed < entries[j].lastUsed
	})

	// Evict oldest shards that are NOT in use
	toEvict := size - dim.maxCache
	evicted := 0

	for i := 0; i < len(entries) && evicted < toEvict; i++ {
		// Only evict if refCount == 0 (not in use)
		if entries[i].refCount == 0 {
			if val, ok := dim.shardCache.LoadAndDelete(entries[i].num); ok {
				shard := val.(*mmapShard)

				// Double-check refcount (race protection)
				if atomic.LoadInt64(&shard.refCount) == 0 {
					dim.unmapShard(shard)
					evicted++
				} else {
					// Someone started using it - put it back
					dim.shardCache.Store(entries[i].num, shard)
				}
			}
		}
	}
}

// releaseShard decrements reference count
func (dim *Manager) releaseShard(shard *mmapShard) {
	if shard == nil || shard.data == nil {
		return
	}

	atomic.AddInt64(&shard.refCount, -1)
}

// ResetCacheStats resets cache statistics (useful for monitoring)
func (dim *Manager) ResetCacheStats() {
	atomic.StoreInt64(&dim.cacheHits, 0)
	atomic.StoreInt64(&dim.cacheMisses, 0)
}

// recordLookupTime records a lookup time (thread-safe)
func (dim *Manager) recordLookupTime(duration time.Duration) {
	micros := duration.Microseconds()

	// Update totals (atomic)
	atomic.AddInt64(&dim.totalLookups, 1)
	atomic.AddInt64(&dim.totalLookupTime, micros)

	// Update circular buffer (with lock)
	dim.lookupTimeLock.Lock()
	dim.recentLookups[dim.recentLookupIdx] = micros
	dim.recentLookupIdx = (dim.recentLookupIdx + 1) % dim.recentLookupSize
	dim.lookupTimeLock.Unlock()
}

// calculateLookupStats calculates performance statistics
func (dim *Manager) calculateLookupStats() map[string]interface{} {
	totalLookups := atomic.LoadInt64(&dim.totalLookups)
	totalTime := atomic.LoadInt64(&dim.totalLookupTime)

	stats := make(map[string]interface{})

	if totalLookups == 0 {
		return stats
	}

	// Overall average (all time)
	avgMicros := float64(totalTime) / float64(totalLookups)
	stats["avg_lookup_time_ms"] = avgMicros / 1000.0
	stats["total_lookups"] = totalLookups

	// Recent statistics (last N lookups)
	dim.lookupTimeLock.Lock()
	recentCopy := make([]int64, dim.recentLookupSize)
	copy(recentCopy, dim.recentLookups)
	dim.lookupTimeLock.Unlock()

	// Find valid entries (non-zero)
	validRecent := make([]int64, 0, dim.recentLookupSize)
	for _, t := range recentCopy {
		if t > 0 {
			validRecent = append(validRecent, t)
		}
	}

	if len(validRecent) > 0 {
		// Sort for percentiles
		sortedRecent := make([]int64, len(validRecent))
		copy(sortedRecent, validRecent)
		sort.Slice(sortedRecent, func(i, j int) bool {
			return sortedRecent[i] < sortedRecent[j]
		})

		// Calculate recent average
		var recentSum int64
		for _, t := range validRecent {
			recentSum += t
		}
		recentAvg := float64(recentSum) / float64(len(validRecent))
		stats["recent_avg_lookup_time_ms"] = recentAvg / 1000.0
		stats["recent_sample_size"] = len(validRecent)

		// Min/Max
		stats["min_lookup_time_ms"] = float64(sortedRecent[0]) / 1000.0
		stats["max_lookup_time_ms"] = float64(sortedRecent[len(sortedRecent)-1]) / 1000.0

		// Percentiles (p50, p95, p99)
		p50idx := len(sortedRecent) * 50 / 100
		p95idx := len(sortedRecent) * 95 / 100
		p99idx := len(sortedRecent) * 99 / 100

		if p50idx < len(sortedRecent) {
			stats["p50_lookup_time_ms"] = float64(sortedRecent[p50idx]) / 1000.0
		}
		if p95idx < len(sortedRecent) {
			stats["p95_lookup_time_ms"] = float64(sortedRecent[p95idx]) / 1000.0
		}
		if p99idx < len(sortedRecent) {
			stats["p99_lookup_time_ms"] = float64(sortedRecent[p99idx]) / 1000.0
		}
	}

	return stats
}

// ResetPerformanceStats resets performance statistics (useful for monitoring periods)
func (dim *Manager) ResetPerformanceStats() {
	atomic.StoreInt64(&dim.cacheHits, 0)
	atomic.StoreInt64(&dim.cacheMisses, 0)
	atomic.StoreInt64(&dim.totalLookups, 0)
	atomic.StoreInt64(&dim.totalLookupTime, 0)

	dim.lookupTimeLock.Lock()
	dim.recentLookups = make([]int64, dim.recentLookupSize)
	dim.recentLookupIdx = 0
	dim.lookupTimeLock.Unlock()
}
