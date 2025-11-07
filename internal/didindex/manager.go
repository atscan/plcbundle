package didindex

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/plcclient"
)

// NewManager creates a new DID index manager
func NewManager(baseDir string, logger Logger) *Manager {
	indexDir := filepath.Join(baseDir, DID_INDEX_DIR)
	shardDir := filepath.Join(indexDir, DID_INDEX_SHARDS)
	configPath := filepath.Join(indexDir, DID_INDEX_CONFIG)

	// Ensure directories exist
	os.MkdirAll(shardDir, 0755)

	// Load or create config
	config, _ := loadIndexConfig(configPath)
	if config == nil {
		config = &Config{
			Version:    DIDINDEX_VERSION,
			Format:     "binary_v1",
			ShardCount: DID_SHARD_COUNT,
			UpdatedAt:  time.Now().UTC(),
		}
	}

	return &Manager{
		baseDir:           baseDir,
		indexDir:          indexDir,
		shardDir:          shardDir,
		configPath:        configPath,
		shardCache:        make(map[uint8]*mmapShard),
		maxCache:          25,
		evictionThreshold: 25,
		config:            config,
		logger:            logger,
	}
}

// Close unmaps all shards and cleans up
func (dim *Manager) Close() error {
	dim.cacheMu.Lock()
	defer dim.cacheMu.Unlock()

	for _, shard := range dim.shardCache {
		dim.unmapShard(shard)
	}

	dim.shardCache = make(map[uint8]*mmapShard)

	return nil
}

func (dim *Manager) SetVerbose(verbose bool) {
	dim.verbose = verbose
}

// GetDIDLocations returns all bundle+position locations for a DID
func (dim *Manager) GetDIDLocations(did string) ([]OpLocation, error) {
	dim.indexMu.RLock()
	defer dim.indexMu.RUnlock()

	// Validate and extract identifier
	identifier, err := extractDIDIdentifier(did)
	if err != nil {
		return nil, err
	}

	// Calculate shard number
	shardNum := dim.calculateShard(identifier)
	if dim.verbose {
		dim.logger.Printf("DEBUG: DID %s -> identifier '%s' -> shard %02x", did, identifier, shardNum)
	}

	// Load shard
	shard, err := dim.loadShard(shardNum)
	if err != nil {
		if dim.verbose {
			dim.logger.Printf("DEBUG: Failed to load shard: %v", err)
		}
		return nil, fmt.Errorf("failed to load shard %02x: %w", shardNum, err)
	}

	if shard.data == nil {
		if dim.verbose {
			dim.logger.Printf("DEBUG: Shard %02x has no data (empty shard)", shardNum)
		}
		return nil, nil
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Shard %02x loaded, size: %d bytes", shardNum, len(shard.data))
	}

	// Binary search
	locations := dim.searchShard(shard, identifier)
	if dim.verbose {
		dim.logger.Printf("DEBUG: Binary search found %d locations", len(locations))
		if len(locations) > 0 {
			dim.logger.Printf("DEBUG: Locations: %v", locations)
		}
	}

	dim.cacheMu.RLock()
	cacheSize := len(dim.shardCache)
	dim.cacheMu.RUnlock()

	if dim.verbose || cacheSize > dim.maxCache {
		dim.logger.Printf("DEBUG: Shard cache size: %d/%d shards (after lookup)", cacheSize, dim.maxCache)
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

// loadShard loads a shard from cache or disk (with mmap)
func (dim *Manager) loadShard(shardNum uint8) (*mmapShard, error) {
	dim.cacheMu.Lock()
	defer dim.cacheMu.Unlock()

	// Check cache
	if shard, exists := dim.shardCache[shardNum]; exists {
		shard.lastUsed = time.Now()
		return shard, nil
	}

	// Load from disk
	shardPath := filepath.Join(dim.shardDir, fmt.Sprintf("%02x.idx", shardNum))

	// Check if file exists
	if _, err := os.Stat(shardPath); os.IsNotExist(err) {
		return &mmapShard{
			shardNum: shardNum,
			data:     nil,
			lastUsed: time.Now(),
		}, nil
	}

	// Open file
	file, err := os.Open(shardPath)
	if err != nil {
		return nil, err
	}

	// Get file size
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
			lastUsed: time.Now(),
		}, nil
	}

	// Memory-map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, int(info.Size()),
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	shard := &mmapShard{
		shardNum: shardNum,
		data:     data,
		file:     file,
		lastUsed: time.Now(),
	}

	// Add to cache
	dim.shardCache[shardNum] = shard

	// Lazy eviction
	if len(dim.shardCache) > dim.evictionThreshold {
		dim.evictMultiple(len(dim.shardCache) - dim.maxCache)
	}

	return shard, nil
}

// evictMultiple evicts multiple shards at once
func (dim *Manager) evictMultiple(count int) {
	if count <= 0 {
		return
	}

	type entry struct {
		num      uint8
		lastUsed time.Time
	}

	entries := make([]entry, 0, len(dim.shardCache))
	for num, shard := range dim.shardCache {
		entries = append(entries, entry{num, shard.lastUsed})
	}

	// Sort by lastUsed (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastUsed.Before(entries[j].lastUsed)
	})

	// Evict oldest 'count' entries
	for i := 0; i < count && i < len(entries); i++ {
		if victim, exists := dim.shardCache[entries[i].num]; exists {
			dim.unmapShard(victim)
			delete(dim.shardCache, entries[i].num)
		}
	}
}

// searchShard performs binary search in a memory-mapped shard
func (dim *Manager) searchShard(shard *mmapShard, identifier string) []OpLocation {
	if shard.data == nil || len(shard.data) < 32 {
		return nil
	}

	data := shard.data

	// Read header
	if string(data[0:4]) != DIDINDEX_MAGIC {
		dim.logger.Printf("Warning: invalid shard magic")
		return nil
	}

	entryCount := binary.LittleEndian.Uint32(data[9:13])
	if entryCount == 0 {
		return nil
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: Searching %d entries for '%s'", entryCount, identifier)
	}

	// Binary search
	left, right := 0, int(entryCount)
	attempts := 0

	for left < right {
		attempts++
		mid := (left + right) / 2

		// O(1) offset lookup
		entryOffset := dim.getEntryOffset(data, mid)
		if entryOffset < 0 {
			if dim.verbose {
				dim.logger.Printf("DEBUG: Invalid entry offset at mid=%d", mid)
			}
			return nil
		}

		// Read identifier at this position
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
		cmp := 0
		if identifier < entryID {
			cmp = -1
		} else if identifier > entryID {
			cmp = 1
		}

		if cmp == 0 {
			if dim.verbose {
				dim.logger.Printf("DEBUG: FOUND at mid=%d after %d attempts", mid, attempts)
			}
			return dim.readLocations(data, entryOffset)
		} else if cmp < 0 {
			right = mid
		} else {
			left = mid + 1
		}
	}

	if dim.verbose {
		dim.logger.Printf("DEBUG: NOT FOUND after %d attempts (left=%d, right=%d)",
			attempts, left, right)
	}

	return nil
}

// getEntryOffset reads entry offset from offset table - O(1) lookup
func (dim *Manager) getEntryOffset(data []byte, entryIndex int) int {
	if len(data) < 32 {
		return -1
	}

	entryCount := binary.LittleEndian.Uint32(data[9:13])
	if entryIndex < 0 || entryIndex >= int(entryCount) {
		return -1
	}

	// Offset table: 4 bytes per entry, starts at byte 32
	offsetPos := 32 + (entryIndex * 4)

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
		if offset+5 > len(data) {
			return locations[:i]
		}

		bundle := binary.LittleEndian.Uint16(data[offset : offset+2])
		position := binary.LittleEndian.Uint16(data[offset+2 : offset+4])
		nullified := data[offset+4] != 0

		locations[i] = OpLocation{
			Bundle:    bundle,
			Position:  position,
			Nullified: nullified,
		}

		offset += 5
	}

	return locations
}

// unmapShard unmaps and closes a shard
func (dim *Manager) unmapShard(shard *mmapShard) {
	if shard.data != nil {
		syscall.Munmap(shard.data)
	}
	if shard.file != nil {
		if f, ok := shard.file.(*os.File); ok {
			f.Close()
		}
	}
}

// GetStats returns index statistics
func (dim *Manager) GetStats() map[string]interface{} {
	dim.cacheMu.RLock()
	defer dim.cacheMu.RUnlock()

	cachedShards := make([]int, 0, len(dim.shardCache))
	for num := range dim.shardCache {
		cachedShards = append(cachedShards, int(num))
	}
	sort.Ints(cachedShards)

	return map[string]interface{}{
		"total_dids":    dim.config.TotalDIDs,
		"last_bundle":   dim.config.LastBundle,
		"shard_count":   dim.config.ShardCount,
		"cached_shards": len(dim.shardCache),
		"cache_limit":   dim.maxCache,
		"cache_order":   cachedShards,
		"updated_at":    dim.config.UpdatedAt,
	}
}

// Exists checks if index exists
func (dim *Manager) Exists() bool {
	_, err := os.Stat(dim.configPath)
	return err == nil
}

// TrimCache trims cache to keep only most recent shard
func (dim *Manager) TrimCache() {
	dim.cacheMu.Lock()
	defer dim.cacheMu.Unlock()

	if len(dim.shardCache) <= 1 {
		return
	}

	// Find most recent shard to keep
	var newestTime time.Time
	var keepNum uint8
	for num, shard := range dim.shardCache {
		if shard.lastUsed.After(newestTime) {
			newestTime = shard.lastUsed
			keepNum = num
		}
	}

	// Evict all except the newest
	for num, shard := range dim.shardCache {
		if num != keepNum {
			dim.unmapShard(shard)
			delete(dim.shardCache, num)
		}
	}
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

// invalidateShard removes a shard from cache
func (dim *Manager) invalidateShard(shardNum uint8) {
	dim.cacheMu.Lock()
	defer dim.cacheMu.Unlock()

	if cached, exists := dim.shardCache[shardNum]; exists {
		dim.unmapShard(cached)
		delete(dim.shardCache, shardNum)
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

// writeShardToPath writes shard to a specific path
func (dim *Manager) writeShardToPath(path string, shardNum uint8, builder *ShardBuilder) error {
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

	return os.WriteFile(path, buf, 0644)
}

// parseShardData parses binary shard data into builder
func (dim *Manager) parseShardData(data []byte, builder *ShardBuilder) error {
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
