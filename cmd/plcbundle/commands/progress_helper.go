package commands

import (
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
)

// NewBundleProgressBar creates a progress bar with auto-calculated bytes from bundles
func NewBundleProgressBar(mgr BundleManager, start, end int) *ui.ProgressBar {
	total := end - start + 1

	// Calculate total bytes from bundle metadata
	index := mgr.GetIndex()
	totalBytes := int64(0)

	for bundleNum := start; bundleNum <= end; bundleNum++ {
		if meta, err := index.GetBundle(bundleNum); err == nil {
			totalBytes += meta.UncompressedSize
		}
	}

	if totalBytes > 0 {
		return ui.NewProgressBarWithBytes(total, totalBytes)
	}

	// Fallback: estimate based on average
	stats := index.GetStats()
	if avgBytes := estimateAvgBundleSize(stats); avgBytes > 0 {
		return ui.NewProgressBarWithBytesAuto(total, avgBytes)
	}

	return ui.NewProgressBar(total)
}

// estimateAvgBundleSize estimates average uncompressed bundle size
func estimateAvgBundleSize(stats map[string]interface{}) int64 {
	if totalUncompressed, ok := stats["total_uncompressed_size"].(int64); ok {
		if bundleCount, ok := stats["bundle_count"].(int); ok && bundleCount > 0 {
			return totalUncompressed / int64(bundleCount)
		}
	}
	return 0
}

// UpdateBundleProgress updates progress with bundle's actual size
func UpdateBundleProgress(pb *ui.ProgressBar, current int, bundle interface{}) {
	// Try to extract size from bundle if available
	type sizer interface {
		GetUncompressedSize() int64
	}

	if b, ok := bundle.(sizer); ok {
		pb.SetWithBytes(current, b.GetUncompressedSize())
	} else {
		pb.Set(current)
	}
}
