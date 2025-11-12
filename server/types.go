package server

import (
	"net/http"
	"time"

	"tangled.org/atscan.net/plcbundle/bundle"
)

// Server serves bundle data over HTTP
type Server struct {
	manager    *bundle.Manager
	addr       string
	config     *Config
	startTime  time.Time
	httpServer *http.Server
}

// Config configures the server
type Config struct {
	Addr            string
	SyncMode        bool
	SyncInterval    time.Duration
	EnableWebSocket bool
	EnableResolver  bool
	Version         string
}

// StatusResponse is the /status endpoint response
type StatusResponse struct {
	Server   ServerStatus    `json:"server"`
	Bundles  BundleStatus    `json:"bundles"`
	Mempool  *MempoolStatus  `json:"mempool,omitempty"`
	DIDIndex *DIDIndexStatus `json:"didindex,omitempty"`
	Resolver *ResolverStatus `json:"resolver,omitempty"`
}

// ServerStatus contains server information
type ServerStatus struct {
	Version             string `json:"version"`
	Origin              string `json:"origin,omitempty"`
	SyncMode            bool   `json:"sync_mode"`
	SyncIntervalSeconds int    `json:"sync_interval_seconds,omitempty"`
	WebSocketEnabled    bool   `json:"websocket_enabled"`
	ResolverEnabled     bool   `json:"resolver_enabled"`
	HandleResolver      string `json:"handle_resolver,omitempty"`
	UptimeSeconds       int    `json:"uptime_seconds"`
}

// DIDIndexStatus contains DID index statistics
type DIDIndexStatus struct {
	Enabled      bool      `json:"enabled"`
	Exists       bool      `json:"exists"`
	TotalDIDs    int64     `json:"total_dids"`
	IndexedDIDs  int64     `json:"indexed_dids"`
	MempoolDIDs  int64     `json:"mempool_dids,omitempty"`
	LastBundle   int       `json:"last_bundle"`
	ShardCount   int       `json:"shard_count"`
	CachedShards int       `json:"cached_shards"`
	CacheLimit   int       `json:"cache_limit"`
	CacheHitRate float64   `json:"cache_hit_rate"`
	CacheHits    int64     `json:"cache_hits"`
	CacheMisses  int64     `json:"cache_misses"`
	TotalLookups int64     `json:"total_lookups"`
	UpdatedAt    time.Time `json:"updated_at"`
	Version      int       `json:"version,omitempty"`
	Format       string    `json:"format,omitempty"`
	HotShards    []int     `json:"hot_shards,omitempty"`

	// Lookup performance metrics
	AvgLookupTimeMs       float64 `json:"avg_lookup_time_ms"`           // All-time average
	RecentAvgLookupTimeMs float64 `json:"recent_avg_lookup_time_ms"`    // Recent average
	MinLookupTimeMs       float64 `json:"min_lookup_time_ms,omitempty"` // Fastest
	MaxLookupTimeMs       float64 `json:"max_lookup_time_ms,omitempty"` // Slowest
	P50LookupTimeMs       float64 `json:"p50_lookup_time_ms,omitempty"` // Median
	P95LookupTimeMs       float64 `json:"p95_lookup_time_ms,omitempty"` // 95th percentile
	P99LookupTimeMs       float64 `json:"p99_lookup_time_ms,omitempty"` // 99th percentile
	RecentSampleSize      int     `json:"recent_sample_size,omitempty"` // How many samples
}

// ResolverStatus contains DID document resolver performance metrics
type ResolverStatus struct {
	Enabled        bool   `json:"enabled"`
	HandleResolver string `json:"handle_resolver,omitempty"`

	// Resolution counts
	TotalResolutions int64   `json:"total_resolutions"`
	MempoolHits      int64   `json:"mempool_hits"`
	BundleHits       int64   `json:"bundle_hits"`
	Errors           int64   `json:"errors"`
	SuccessRate      float64 `json:"success_rate"`
	MempoolHitRate   float64 `json:"mempool_hit_rate"`

	// Overall timing (all-time averages)
	AvgTotalTimeMs   float64 `json:"avg_total_time_ms"`
	AvgMempoolTimeMs float64 `json:"avg_mempool_time_ms"`
	AvgIndexTimeMs   float64 `json:"avg_index_time_ms,omitempty"`
	AvgLoadOpTimeMs  float64 `json:"avg_load_op_time_ms,omitempty"`

	// Recent performance (last N resolutions)
	RecentAvgTotalTimeMs   float64 `json:"recent_avg_total_time_ms"`
	RecentAvgMempoolTimeMs float64 `json:"recent_avg_mempool_time_ms"`
	RecentAvgIndexTimeMs   float64 `json:"recent_avg_index_time_ms,omitempty"`
	RecentAvgLoadTimeMs    float64 `json:"recent_avg_load_time_ms,omitempty"`
	RecentSampleSize       int     `json:"recent_sample_size"`

	// Percentiles (total response time)
	MinTotalTimeMs float64 `json:"min_total_time_ms,omitempty"`
	MaxTotalTimeMs float64 `json:"max_total_time_ms,omitempty"`
	P50TotalTimeMs float64 `json:"p50_total_time_ms,omitempty"`
	P95TotalTimeMs float64 `json:"p95_total_time_ms,omitempty"`
	P99TotalTimeMs float64 `json:"p99_total_time_ms,omitempty"`

	// Breakdown percentiles (for bundle resolutions only)
	P95IndexTimeMs  float64 `json:"p95_index_time_ms,omitempty"`
	P95LoadOpTimeMs float64 `json:"p95_load_op_time_ms,omitempty"`
}

// BundleStatus contains bundle statistics
type BundleStatus struct {
	Count            int       `json:"count"`
	FirstBundle      int       `json:"first_bundle,omitempty"`
	LastBundle       int       `json:"last_bundle,omitempty"`
	TotalSize        int64     `json:"total_size"`
	UncompressedSize int64     `json:"uncompressed_size,omitempty"`
	CompressionRatio float64   `json:"compression_ratio,omitempty"`
	TotalOperations  int       `json:"total_operations,omitempty"`
	AvgOpsPerHour    int       `json:"avg_ops_per_hour,omitempty"`
	StartTime        time.Time `json:"start_time,omitempty"`
	EndTime          time.Time `json:"end_time,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
	HeadAgeSeconds   int       `json:"head_age_seconds,omitempty"`
	RootHash         string    `json:"root_hash,omitempty"`
	HeadHash         string    `json:"head_hash,omitempty"`
	Gaps             int       `json:"gaps,omitempty"`
	HasGaps          bool      `json:"has_gaps"`
	GapNumbers       []int     `json:"gap_numbers,omitempty"`
}

// MempoolStatus contains mempool statistics
type MempoolStatus struct {
	Count                int       `json:"count"`
	TargetBundle         int       `json:"target_bundle"`
	CanCreateBundle      bool      `json:"can_create_bundle"`
	MinTimestamp         time.Time `json:"min_timestamp"`
	Validated            bool      `json:"validated"`
	ProgressPercent      float64   `json:"progress_percent"`
	BundleSize           int       `json:"bundle_size"`
	OperationsNeeded     int       `json:"operations_needed"`
	FirstTime            time.Time `json:"first_time,omitempty"`
	LastTime             time.Time `json:"last_time,omitempty"`
	TimespanSeconds      int       `json:"timespan_seconds,omitempty"`
	LastOpAgeSeconds     int       `json:"last_op_age_seconds,omitempty"`
	EtaNextBundleSeconds int       `json:"eta_next_bundle_seconds,omitempty"`
}

// RequestLog represents a logged HTTP request
type RequestLog struct {
	Timestamp  time.Time `json:"timestamp"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	UserAgent  string    `json:"user_agent"`
	RemoteAddr string    `json:"remote_addr"`
}
