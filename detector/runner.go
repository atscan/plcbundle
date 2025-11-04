// detector/runner.go
package detector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/plc"
)

// Runner executes detectors against operations
type Runner struct {
	registry *Registry
	config   *Config
	logger   Logger
}

type Logger interface {
	Printf(format string, v ...interface{})
}

// NewRunner creates a new detector runner
func NewRunner(registry *Registry, config *Config, logger Logger) *Runner {
	if config == nil {
		config = DefaultConfig()
	}
	return &Runner{
		registry: registry,
		config:   config,
		logger:   logger,
	}
}

// RunOnBundle runs detector(s) on all operations in a bundle
func (r *Runner) RunOnBundle(ctx context.Context, detectorName string, b *bundle.Bundle) ([]*Result, error) {
	detector, err := r.registry.Get(detectorName)
	if err != nil {
		return nil, err
	}

	var results []*Result

	if r.config.Parallel {
		results = r.runParallel(ctx, detector, b)
	} else {
		results = r.runSequential(ctx, detector, b)
	}

	// Filter by minimum confidence
	filtered := make([]*Result, 0)
	for _, res := range results {
		if res.Match != nil && res.Match.Confidence >= r.config.MinConfidence {
			filtered = append(filtered, res)
		}
	}

	return filtered, nil
}

func (r *Runner) runSequential(ctx context.Context, detector Detector, b *bundle.Bundle) []*Result {
	results := make([]*Result, 0)

	for pos, op := range b.Operations {
		select {
		case <-ctx.Done():
			return results
		default:
		}

		result := r.detectOne(ctx, detector, b.BundleNumber, pos, op)
		if result.Match != nil || result.Error != nil {
			results = append(results, result)
		}
	}

	return results
}

func (r *Runner) runParallel(ctx context.Context, detector Detector, b *bundle.Bundle) []*Result {
	type job struct {
		pos int
		op  plc.PLCOperation
	}

	jobs := make(chan job, len(b.Operations))
	resultsChan := make(chan *Result, len(b.Operations))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < r.config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				result := r.detectOne(ctx, detector, b.BundleNumber, j.pos, j.op)
				if result.Match != nil || result.Error != nil {
					resultsChan <- result
				}
			}
		}()
	}

	// Send jobs
	for pos, op := range b.Operations {
		jobs <- job{pos: pos, op: op}
	}
	close(jobs)

	// Wait for completion
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	results := make([]*Result, 0)
	for result := range resultsChan {
		results = append(results, result)
	}

	return results
}

func (r *Runner) detectOne(ctx context.Context, detector Detector, bundleNum, pos int, op plc.PLCOperation) *Result {
	// Create timeout context
	detectCtx, cancel := context.WithTimeout(ctx, r.config.Timeout)
	defer cancel()

	result := &Result{
		BundleNumber: bundleNum,
		Position:     pos,
		DID:          op.DID,
		CID:          op.CID,
		DetectorName: detector.Name(),
		DetectedAt:   time.Now(),
	}

	match, err := detector.Detect(detectCtx, op)
	result.Match = match
	result.Error = err

	return result
}

// RunMultipleDetectors runs multiple detectors on a bundle
func (r *Runner) RunMultipleDetectors(ctx context.Context, detectorNames []string, b *bundle.Bundle) (map[string][]*Result, error) {
	allResults := make(map[string][]*Result)

	for _, name := range detectorNames {
		results, err := r.RunOnBundle(ctx, name, b)
		if err != nil {
			return nil, fmt.Errorf("detector %s failed: %w", name, err)
		}
		allResults[name] = results
	}

	return allResults, nil
}

// Stats represents detection statistics
type Stats struct {
	TotalOperations int
	MatchedCount    int
	MatchRate       float64
	ByReason        map[string]int
	ByCategory      map[string]int
	ByConfidence    map[string]int // 0.9-1.0, 0.8-0.9, etc.
}

// CalculateStats computes statistics from results
func CalculateStats(results []*Result, totalOps int) *Stats {
	stats := &Stats{
		TotalOperations: totalOps,
		MatchedCount:    len(results),
		ByReason:        make(map[string]int),
		ByCategory:      make(map[string]int),
		ByConfidence:    make(map[string]int),
	}

	if totalOps > 0 {
		stats.MatchRate = float64(len(results)) / float64(totalOps)
	}

	for _, res := range results {
		if res.Match == nil {
			continue
		}

		stats.ByReason[res.Match.Reason]++
		stats.ByCategory[res.Match.Category]++

		// Confidence buckets
		conf := res.Match.Confidence
		switch {
		case conf >= 0.95:
			stats.ByConfidence["0.95-1.00"]++
		case conf >= 0.90:
			stats.ByConfidence["0.90-0.95"]++
		case conf >= 0.85:
			stats.ByConfidence["0.85-0.90"]++
		default:
			stats.ByConfidence["0.00-0.85"]++
		}
	}

	return stats
}
