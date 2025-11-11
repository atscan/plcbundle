package didindex

import (
	"context"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

// BundleProvider is an interface to avoid circular dependencies
// The bundle.Manager implements this interface
type BundleProvider interface {
	LoadBundleForDIDIndex(ctx context.Context, bundleNumber int) (*BundleData, error)
	LoadOperation(ctx context.Context, bundleNumber int, position int) (*plcclient.PLCOperation, error)
	LoadOperations(ctx context.Context, bundleNumber int, positions []int) (map[int]*plcclient.PLCOperation, error)
	GetBundleIndex() BundleIndexProvider
}

// BundleData contains the minimal bundle information needed by DID index
type BundleData struct {
	BundleNumber int
	Operations   []plcclient.PLCOperation
}

// BundleIndexProvider provides access to bundle metadata
type BundleIndexProvider interface {
	GetBundles() []*BundleMetadata
	GetBundle(bundleNumber int) (*BundleMetadata, error)
	GetLastBundle() *BundleMetadata
}

// BundleMetadata is the minimal metadata needed by DID index
type BundleMetadata struct {
	BundleNumber int
	StartTime    time.Time
	EndTime      time.Time
}

// Logger interface (shared)
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
