package plcbundle

import (
	"encoding/json"
	"os"
	"time"
)

type Index struct {
	Version    string         `json:"version"`
	LastBundle int            `json:"last_bundle"`
	UpdatedAt  time.Time      `json:"updated_at"`
	Bundles    []*BundleEntry `json:"bundles"`
}

type BundleEntry struct {
	BundleNumber     int       `json:"bundle_number"`
	StartTime        time.Time `json:"start_time"`
	EndTime          time.Time `json:"end_time"`
	OperationCount   int       `json:"operation_count"`
	DIDCount         int       `json:"did_count"`
	Hash             string    `json:"hash"`
	CompressedHash   string    `json:"compressed_hash"`
	CompressedSize   int64     `json:"compressed_size"`
	UncompressedSize int64     `json:"uncompressed_size"`
	Cursor           string    `json:"cursor"`
	PrevBundleHash   string    `json:"prev_bundle_hash,omitempty"`
}

func LoadIndex(path string) (*Index, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var idx Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, err
	}

	return &idx, nil
}

func (idx *Index) SaveToFile(path string) error {
	idx.UpdatedAt = time.Now()
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
