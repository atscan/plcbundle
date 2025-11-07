package bundle

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/plc"
)

const (
	DID_INDEX_DIR      = ".plcbundle"
	DID_INDEX_SHARDS   = "shards"
	DID_INDEX_CONFIG   = "config.json"
	DID_SHARD_COUNT    = 256
	DID_PREFIX         = "did:plc:"
	DID_IDENTIFIER_LEN = 24 // Without "did:plc:" prefix

	// Binary format constants (renamed to avoid conflict)
	DIDINDEX_MAGIC   = "PLCD"
	DIDINDEX_VERSION = 2
)

// DIDIndexManager manages sharded DID position indexes with mmap
type DIDIndexManager struct {
	baseDir    string
	indexDir   string
	shardDir   string
	configPath string

	// LRU cache for hot shards
	shardCache        map[uint8]*mmapShard
	maxCache          int
	cacheMu           sync.RWMutex
	evictionThreshold int

	config  *DIDIndexConfig
	logger  Logger
	verbose bool

	indexMu sync.RWMutex
}

// mmapShard represents a memory-mapped shard file
type mmapShard struct {
	shardNum uint8
	data     []byte
	file     *os.File
	lastUsed time.Time
}

// DIDIndexConfig stores index metadata
type DIDIndexConfig struct {
	Version    int       `json:"version"`
	Format     string    `json:"format"`
	ShardCount int       `json:"shard_count"`
	TotalDIDs  int64     `json:"total_dids"`
	UpdatedAt  time.Time `json:"updated_at"`
	LastBundle int       `json:"last_bundle"`
}

// OpLocation represents exact location of an operation
type OpLocation struct {
	Bundle    uint16
	Position  uint16
	Nullified bool
}

// NewDIDIndexManager creates a new DID index manager
func NewDIDIndexManager(baseDir string, logger Logger) *DIDIndexManager {
	indexDir := filepath.Join(baseDir, DID_INDEX_DIR)
	shardDir := filepath.Join(indexDir, DID_INDEX_SHARDS)
	configPath := filepath.Join(indexDir, DID_INDEX_CONFIG)

	// Ensure directories exist
	os.MkdirAll(shardDir, 0755)

	// Load or create config
	config, _ := loadIndexConfig(configPath)
	if config == nil {
		config = &DIDIndexConfig{
			Version:    DIDINDEX_VERSION,
			Format:     "binary_v1",
			ShardCount: DID_SHARD_COUNT,
			UpdatedAt:  time.Now().UTC(),
		}
	}

	return &DIDIndexManager{
		baseDir:           baseDir,
		indexDir:          indexDir,
		shardDir:          shardDir,
		configPath:        configPath,
		shardCache:        make(map[uint8]*mmapShard),
		maxCache:          25, // Keep 20 hot shards in memory (~120 MB)
		evictionThreshold: 25,
		config:            config,
		logger:            logger,
	}
}

// Close unmaps all shards and cleans up
func (dim *DIDIndexManager) Close() error {
	dim.cacheMu.Lock()
	defer dim.cacheMu.Unlock()

	for _, shard := range dim.shardCache {
		dim.unmapShard(shard)
	}

	dim.shardCache = make(map[uint8]*mmapShard)

	return nil
}

func (dim *DIDIndexManager) SetVerbose(verbose bool) {
	dim.verbose = verbose
}

// GetDIDLocations returns all bundle+position locations for a DID
func (dim *DIDIndexManager) GetDIDLocations(did string) ([]OpLocation, error) {
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
func (dim *DIDIndexManager) calculateShard(identifier string) uint8 {
	h := fnv.New32a()
	h.Write([]byte(identifier))
	hash := h.Sum32()
	return uint8(hash % DID_SHARD_COUNT)
}

// loadShard loads a shard from cache or disk (with mmap)
func (dim *DIDIndexManager) loadShard(shardNum uint8) (*mmapShard, error) {
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
		// Shard doesn't exist yet (no DIDs in this shard)
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

	// Lazy eviction: only evict when significantly over limit
	if len(dim.shardCache) > dim.evictionThreshold {
		dim.evictMultiple(len(dim.shardCache) - dim.maxCache)
	}

	return shard, nil
}

// Evict multiple shards at once to reduce eviction frequency
func (dim *DIDIndexManager) evictMultiple(count int) {
	if count <= 0 {
		return
	}

	// Build list of (shardNum, lastUsed) and sort
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
func (dim *DIDIndexManager) searchShard(shard *mmapShard, identifier string) []OpLocation {
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

		// ✨ O(1) offset lookup
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
func (dim *DIDIndexManager) getEntryOffset(data []byte, entryIndex int) int {
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
func (dim *DIDIndexManager) readLocations(data []byte, offset int) []OpLocation {
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
func (dim *DIDIndexManager) unmapShard(shard *mmapShard) {
	if shard.data != nil {
		syscall.Munmap(shard.data)
	}
	if shard.file != nil {
		shard.file.Close()
	}
}

// GetStats returns index statistics
func (dim *DIDIndexManager) GetStats() map[string]interface{} {
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
		"cache_order":   cachedShards, // Still works, just sorted differently
		"updated_at":    dim.config.UpdatedAt,
	}
}

// Exists checks if index exists
func (dim *DIDIndexManager) Exists() bool {
	_, err := os.Stat(dim.configPath)
	return err == nil
}

// extractDIDIdentifier extracts the 24-char identifier from full DID
func extractDIDIdentifier(did string) (string, error) {
	if err := plc.ValidateDIDFormat(did); err != nil {
		return "", err
	}

	// Remove "did:plc:" prefix
	identifier := did[8:] // Skip "did:plc:"

	if len(identifier) != DID_IDENTIFIER_LEN {
		return "", fmt.Errorf("invalid identifier length: %d", len(identifier))
	}

	return identifier, nil
}

// loadIndexConfig loads index configuration
func loadIndexConfig(path string) (*DIDIndexConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config DIDIndexConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// saveIndexConfig saves index configuration
func (dim *DIDIndexManager) saveIndexConfig() error {
	dim.config.UpdatedAt = time.Now().UTC()

	data, err := json.MarshalIndent(dim.config, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(dim.configPath, data, 0644)
}

// Add this debug command to verify shard integrity

func (dim *DIDIndexManager) DebugShard(shardNum uint8) error {
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

	// Show first few entries using offset table
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

func (dim *DIDIndexManager) TrimCache() {
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

// GetConfig returns the index configuration (for version checking)
func (dim *DIDIndexManager) GetConfig() *DIDIndexConfig {
	return dim.config
}
