package bundle

import (
	"time"

	"tangled.org/atscan.net/plcbundle/plcclient"
)

// CreateBundle creates a complete bundle structure from operations
func (m *Manager) CreateBundle(bundleNumber int, operations []plcclient.PLCOperation, cursor string, parent string) *Bundle {
	if len(operations) != BUNDLE_SIZE {
		m.logger.Printf("Warning: bundle has %d operations, expected %d", len(operations), BUNDLE_SIZE)
	}

	dids := m.operations.ExtractUniqueDIDs(operations)
	_, boundaryCIDs := m.operations.GetBoundaryCIDs(operations)

	// Convert boundary CIDs map to slice
	cidSlice := make([]string, 0, len(boundaryCIDs))
	for cid := range boundaryCIDs {
		cidSlice = append(cidSlice, cid)
	}

	bundle := &Bundle{
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

	return bundle
}
