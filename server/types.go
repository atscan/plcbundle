package server

import "time"

// StatusResponse is the /status endpoint response
type StatusResponse struct {
	Bundles BundleStatus   `json:"bundles"`
	Mempool *MempoolStatus `json:"mempool,omitempty"`
	Server  ServerStatus   `json:"server"`
}

// ServerStatus contains server information
type ServerStatus struct {
	Version             string `json:"version"`
	UptimeSeconds       int    `json:"uptime_seconds"`
	SyncMode            bool   `json:"sync_mode"`
	SyncIntervalSeconds int    `json:"sync_interval_seconds,omitempty"`
	WebSocketEnabled    bool   `json:"websocket_enabled"`
	Origin              string `json:"origin,omitempty"`
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
