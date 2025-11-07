package sync

import (
	"time"

	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/storage"
)

// CreateBundle creates a bundle structure from operations
// Note: This doesn't do hashing - that's done by Manager.SaveBundle
func CreateBundle(
	bundleNumber int,
	operations []plcclient.PLCOperation,
	cursor string,
	parent string,
	ops *storage.Operations,
) *Bundle {

	dids := ops.ExtractUniqueDIDs(operations)
	_, boundaryCIDs := ops.GetBoundaryCIDs(operations)

	cidSlice := make([]string, 0, len(boundaryCIDs))
	for cid := range boundaryCIDs {
		cidSlice = append(cidSlice, cid)
	}

	return &Bundle{
		BundleNumber: bundleNumber,
		StartTime:    operations[0].CreatedAt,
		EndTime:      operations[len(operations)-1].CreatedAt,
		Operations:   operations,
		DIDCount:     len(dids),
		Cursor:       cursor,
		Parent:       parent,
		BoundaryCIDs: cidSlice,
		Compressed:   true,
		CreatedAt:    time.Now().UTC(),
	}
}

// Bundle is defined here temporarily - move to parent package later
type Bundle struct {
	BundleNumber int
	StartTime    time.Time
	EndTime      time.Time
	Operations   []plcclient.PLCOperation
	DIDCount     int

	Hash        string
	ContentHash string
	Parent      string

	CompressedHash   string
	CompressedSize   int64
	UncompressedSize int64
	Cursor           string
	BoundaryCIDs     []string
	Compressed       bool
	CreatedAt        time.Time
}
