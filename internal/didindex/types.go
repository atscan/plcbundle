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
	DIDINDEX_VERSION = 3

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

// OpLocation represents exact location of an operation
type OpLocation struct {
	Bundle    uint16
	Position  uint16
	Nullified bool
}

// ShardBuilder accumulates DID positions for a shard
type ShardBuilder struct {
	entries map[string][]OpLocation
	mu      sync.Mutex
}

// OpLocationWithOperation contains an operation with its bundle/position
type OpLocationWithOperation struct {
	Operation plcclient.PLCOperation
	Bundle    int
	Position  int
}
