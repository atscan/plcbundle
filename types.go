package plcbundle

import (
	"time"

	"tangled.org/atscan.net/plcbundle/plcclient"
)

// Bundle represents a PLC bundle (public version)
type Bundle struct {
	BundleNumber     int
	StartTime        time.Time
	EndTime          time.Time
	Operations       []plcclient.PLCOperation
	DIDCount         int
	Hash             string
	CompressedSize   int64
	UncompressedSize int64
}

// BundleInfo provides metadata about a bundle
type BundleInfo struct {
	BundleNumber     int       `json:"bundle_number"`
	StartTime        time.Time `json:"start_time"`
	EndTime          time.Time `json:"end_time"`
	OperationCount   int       `json:"operation_count"`
	DIDCount         int       `json:"did_count"`
	Hash             string    `json:"hash"`
	CompressedSize   int64     `json:"compressed_size"`
	UncompressedSize int64     `json:"uncompressed_size"`
}

// IndexStats provides statistics about the bundle index
type IndexStats struct {
	BundleCount    int
	FirstBundle    int
	LastBundle     int
	TotalSize      int64
	MissingBundles []int
}

// Helper to convert internal bundle to public
func toBundlePublic(b interface{}) *Bundle {
	// Implement conversion from internal bundle to public Bundle
	return &Bundle{} // placeholder
}
