package bundleindex

import "time"

// BundleMetadata represents metadata about a bundle
type BundleMetadata struct {
	BundleNumber   int       `json:"bundle_number"`
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time"`
	OperationCount int       `json:"operation_count"`
	DIDCount       int       `json:"did_count"`

	// Primary hash - cumulative chain hash (includes all history)
	Hash string `json:"hash"`

	// Content hash - SHA256 of bundle operations only
	ContentHash string `json:"content_hash"`

	// Parent chain hash - links to previous bundle
	Parent string `json:"parent,omitempty"`

	// File hashes and sizes
	CompressedHash   string    `json:"compressed_hash"`
	CompressedSize   int64     `json:"compressed_size"`
	UncompressedSize int64     `json:"uncompressed_size"`
	Cursor           string    `json:"cursor"`
	CreatedAt        time.Time `json:"created_at"`
}
