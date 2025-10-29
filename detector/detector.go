// detector/detector.go
package detector

import (
	"context"
	"time"

	"tangled.org/atscan.net/plcbundle/plc"
)

// Detector represents a spam detection algorithm
type Detector interface {
	// Name returns the detector's unique identifier
	Name() string

	// Description returns a human-readable description
	Description() string

	// Detect analyzes an operation and returns a match result
	Detect(ctx context.Context, op plc.PLCOperation) (*Match, error)

	// Version returns the detector version
	Version() string
}

// Match represents a positive spam detection
type Match struct {
	Reason     string                 // Short identifier (e.g., "nostr_crosspost")
	Category   string                 // Broader category (e.g., "cross_posting")
	Confidence float64                // 0.0 to 1.0
	Note       string                 // Optional human-readable explanation
	Metadata   map[string]interface{} // Additional context
}

// Result represents the outcome of running a detector on an operation
type Result struct {
	BundleNumber int
	Position     int
	DID          string
	CID          string // ← Add this field
	Match        *Match // nil if no match
	Error        error
	DetectorName string
	DetectedAt   time.Time
}

// Config holds detector configuration
type Config struct {
	MinConfidence float64
	Timeout       time.Duration
	Parallel      bool
	Workers       int
}

// DefaultConfig returns sensible defaults
func DefaultConfig() *Config {
	return &Config{
		MinConfidence: 0.90,
		Timeout:       5 * time.Second,
		Parallel:      true,
		Workers:       4,
	}
}
