package didindex

import (
	"sync"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

const (
	DID_INDEX_DIR      = ".plcbundle"
	DID_INDEX_SHARDS   = "shards"
	DID_INDEX_CONFIG   = "config.json"
	DID_SHARD_COUNT    = 256
	DID_PREFIX         = "did:plc:"
	DID_IDENTIFIER_LEN = 24 // Without "did:plc:" prefix

	// Binary format constants
	DIDINDEX_MAGIC   = "PLCD"
	DIDINDEX_VERSION = 4

	// Format sizes
	LOCATION_SIZE_V3 = 5 // Old: 2+2+1
	LOCATION_SIZE_V4 = 4 // New: packed uint32

	BUILD_BATCH_SIZE = 100 // Process 100 bundles at a time

	PREFIX_INDEX_SIZE = 256 // Index first byte of identifier (0x00-0xFF)
)

// Manager manages sharded DID position indexes with mmap
type Manager struct {
	baseDir    string
	indexDir   string
	shardDir   string
	configPath string

	// LRU cache for hot shards
	shardCache        map[uint8]*mmapShard
	maxCache          int
	cacheMu           sync.RWMutex
	evictionThreshold int

	config  *Config
	logger  Logger
	verbose bool

	indexMu sync.RWMutex
}

// mmapShard represents a memory-mapped shard file
type mmapShard struct {
	shardNum    uint8
	data        []byte
	file        interface{} // *os.File (avoid import)
	lastUsed    time.Time
	accessCount int64
}

// Config stores index metadata
type Config struct {
	Version    int       `json:"version"`
	Format     string    `json:"format"`
	ShardCount int       `json:"shard_count"`
	TotalDIDs  int64     `json:"total_dids"`
	UpdatedAt  time.Time `json:"updated_at"`
	LastBundle int       `json:"last_bundle"`
}

// ShardBuilder accumulates DID positions for a shard
type ShardBuilder struct {
	entries map[string][]OpLocation
	mu      sync.Mutex
}

// OpLocation represents exact location of an operation
type OpLocation uint32

// OpLocationWithOperation contains an operation with its bundle/position
type OpLocationWithOperation struct {
	Operation plcclient.PLCOperation
	Bundle    int
	Position  int
}

func NewOpLocation(bundle, position uint16, nullified bool) OpLocation {
	globalPos := uint32(bundle)*10000 + uint32(position)
	loc := globalPos << 1
	if nullified {
		loc |= 1
	}
	return OpLocation(loc)
}

// Getters
func (loc OpLocation) GlobalPosition() uint32 {
	return uint32(loc) >> 1
}

func (loc OpLocation) Bundle() uint16 {
	return uint16(loc.GlobalPosition() / 10000)
}

func (loc OpLocation) Position() uint16 {
	return uint16(loc.GlobalPosition() % 10000)
}

func (loc OpLocation) Nullified() bool {
	return (loc & 1) == 1
}

func (loc OpLocation) IsAfter(other OpLocation) bool {
	// Compare global positions directly
	return loc.GlobalPosition() > other.GlobalPosition()
}

func (loc OpLocation) IsBefore(other OpLocation) bool {
	return loc.GlobalPosition() < other.GlobalPosition()
}

func (loc OpLocation) Equals(other OpLocation) bool {
	// Compare entire packed value (including nullified bit)
	return loc == other
}

func (loc OpLocation) PositionEquals(other OpLocation) bool {
	// Compare only position (ignore nullified bit)
	return loc.GlobalPosition() == other.GlobalPosition()
}

// Convenience conversions
func (loc OpLocation) BundleInt() int {
	return int(loc.Bundle())
}

func (loc OpLocation) PositionInt() int {
	return int(loc.Position())
}

// For sorting/comparison
func (loc OpLocation) Less(other OpLocation) bool {
	return loc.GlobalPosition() < other.GlobalPosition()
}
