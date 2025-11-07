package sync

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/storage"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

// Cloner handles cloning bundles from remote endpoints
type Cloner struct {
	operations *storage.Operations
	bundleDir  string
	logger     types.Logger
}

// NewCloner creates a new cloner
func NewCloner(operations *storage.Operations, bundleDir string, logger types.Logger) *Cloner {
	return &Cloner{
		operations: operations,
		bundleDir:  bundleDir,
		logger:     logger,
	}
}

// CloneOptions configures cloning behavior
type CloneOptions struct {
	RemoteURL    string
	Workers      int
	SkipExisting bool
	ProgressFunc func(downloaded, total int, bytesDownloaded, bytesTotal int64)
	SaveInterval time.Duration
	Verbose      bool
	Logger       types.Logger
}

// CloneResult contains cloning results
type CloneResult struct {
	RemoteBundles int
	Downloaded    int
	Failed        int
	Skipped       int
	TotalBytes    int64
	Duration      time.Duration
	Interrupted   bool
	FailedBundles []int
}

// Clone performs the cloning operation
func (c *Cloner) Clone(
	ctx context.Context,
	opts CloneOptions,
	localIndex *bundleindex.Index,
	updateIndex func([]int, map[int]*bundleindex.BundleMetadata, bool) error,
) (*CloneResult, error) {

	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.SaveInterval <= 0 {
		opts.SaveInterval = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = c.logger
	}

	result := &CloneResult{}
	startTime := time.Now()

	// Step 1: Fetch remote index
	opts.Logger.Printf("Fetching remote index from %s", opts.RemoteURL)
	remoteIndex, err := c.loadRemoteIndex(opts.RemoteURL)
	if err != nil {
		return nil, fmt.Errorf("failed to load remote index: %w", err)
	}

	remoteBundles := remoteIndex.GetBundles()
	if len(remoteBundles) == 0 {
		opts.Logger.Printf("Remote has no bundles")
		return result, nil
	}

	result.RemoteBundles = len(remoteBundles)
	opts.Logger.Printf("Remote has %d bundles", len(remoteBundles))

	// Step 2: Determine bundles to download
	localBundleMap := make(map[int]*bundleindex.BundleMetadata)
	for _, meta := range localIndex.GetBundles() {
		localBundleMap[meta.BundleNumber] = meta
	}

	remoteBundleMap := make(map[int]*bundleindex.BundleMetadata)
	for _, meta := range remoteBundles {
		remoteBundleMap[meta.BundleNumber] = meta
	}

	var bundlesToDownload []int
	var totalBytes int64

	for _, meta := range remoteBundles {
		if opts.SkipExisting && localBundleMap[meta.BundleNumber] != nil {
			result.Skipped++
			if opts.Verbose {
				opts.Logger.Printf("Skipping existing bundle %06d", meta.BundleNumber)
			}
			continue
		}
		bundlesToDownload = append(bundlesToDownload, meta.BundleNumber)
		totalBytes += meta.CompressedSize
	}

	if len(bundlesToDownload) == 0 {
		opts.Logger.Printf("All bundles already exist locally")
		return result, nil
	}

	opts.Logger.Printf("Downloading %d bundles (%d bytes)", len(bundlesToDownload), totalBytes)

	// Step 3: Set up periodic index saving
	saveCtx, saveCancel := context.WithCancel(ctx)
	defer saveCancel()

	var downloadedBundles []int
	var downloadedMu sync.Mutex

	saveDone := make(chan struct{})
	go func() {
		defer close(saveDone)
		ticker := time.NewTicker(opts.SaveInterval)
		defer ticker.Stop()

		for {
			select {
			case <-saveCtx.Done():
				return
			case <-ticker.C:
				downloadedMu.Lock()
				bundles := make([]int, len(downloadedBundles))
				copy(bundles, downloadedBundles)
				downloadedMu.Unlock()

				if opts.Verbose {
					opts.Logger.Printf("Periodic save: updating index with %d bundles", len(bundles))
				}
				updateIndex(bundles, remoteBundleMap, false)
			}
		}
	}()

	// Step 4: Download bundles
	successList, failedList, bytes := c.downloadBundlesConcurrent(
		ctx,
		opts.RemoteURL,
		bundlesToDownload,
		remoteBundleMap,
		totalBytes,
		opts.Workers,
		opts.ProgressFunc,
		opts.Verbose,
		&downloadedBundles,
		&downloadedMu,
	)

	result.Downloaded = len(successList)
	result.Failed = len(failedList)
	result.TotalBytes = bytes
	result.FailedBundles = failedList
	result.Interrupted = ctx.Err() != nil

	// Stop periodic saves
	saveCancel()
	<-saveDone

	// Step 5: Final index update
	opts.Logger.Printf("Updating local index...")
	if err := updateIndex(successList, remoteBundleMap, opts.Verbose); err != nil {
		return result, fmt.Errorf("failed to update index: %w", err)
	}

	result.Duration = time.Since(startTime)
	return result, nil
}

// loadRemoteIndex loads index from remote URL
func (c *Cloner) loadRemoteIndex(baseURL string) (*bundleindex.Index, error) {
	indexURL := strings.TrimSuffix(baseURL, "/") + "/index.json"

	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Get(indexURL)
	if err != nil {
		return nil, fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var idx bundleindex.Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

	return &idx, nil
}

// downloadBundlesConcurrent downloads bundles using worker pool
func (c *Cloner) downloadBundlesConcurrent(
	ctx context.Context,
	baseURL string,
	bundleNumbers []int,
	remoteBundleMap map[int]*bundleindex.BundleMetadata,
	totalBytes int64,
	workers int,
	progressFunc func(downloaded, total int, bytesDownloaded, bytesTotal int64),
	verbose bool,
	downloadedBundles *[]int,
	downloadedMu *sync.Mutex,
) (successList []int, failedList []int, downloadedBytes int64) {

	type job struct {
		bundleNum    int
		expectedHash string
	}

	type result struct {
		bundleNum int
		success   bool
		bytes     int64
		err       error
	}

	jobs := make(chan job, len(bundleNumbers))
	results := make(chan result, len(bundleNumbers))

	// Shared state
	var (
		mu             sync.Mutex
		processedCount int
		processedBytes int64
		success        []int
		failed         []int
	)

	// Start workers
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 120 * time.Second}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				// Check cancellation
				select {
				case <-ctx.Done():
					results <- result{
						bundleNum: j.bundleNum,
						success:   false,
						err:       ctx.Err(),
					}
					continue
				default:
				}

				// Download bundle
				bytes, err := c.downloadBundle(client, baseURL, j.bundleNum, j.expectedHash)

				// Update progress
				mu.Lock()
				processedCount++
				if err == nil {
					processedBytes += bytes
					success = append(success, j.bundleNum)
					if downloadedMu != nil && downloadedBundles != nil {
						downloadedMu.Lock()
						*downloadedBundles = append(*downloadedBundles, j.bundleNum)
						downloadedMu.Unlock()
					}
				} else {
					failed = append(failed, j.bundleNum)
				}

				if progressFunc != nil {
					progressFunc(processedCount, len(bundleNumbers), processedBytes, totalBytes)
				}
				mu.Unlock()

				results <- result{
					bundleNum: j.bundleNum,
					success:   err == nil,
					bytes:     bytes,
					err:       err,
				}
			}
		}()
	}

	// Send jobs
	for _, num := range bundleNumbers {
		expectedHash := ""
		if meta, exists := remoteBundleMap[num]; exists {
			expectedHash = meta.CompressedHash
		}
		jobs <- job{
			bundleNum:    num,
			expectedHash: expectedHash,
		}
	}
	close(jobs)

	// Wait for completion
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for res := range results {
		if res.err != nil && res.err != context.Canceled {
			c.logger.Printf("Failed to download bundle %06d: %v", res.bundleNum, res.err)
		} else if res.success && verbose {
			c.logger.Printf("✓ Downloaded and verified bundle %06d (%d bytes)", res.bundleNum, res.bytes)
		}
	}

	mu.Lock()
	successList = success
	failedList = failed
	downloadedBytes = processedBytes
	mu.Unlock()

	return
}

// downloadBundle downloads a single bundle and verifies hash
func (c *Cloner) downloadBundle(client *http.Client, baseURL string, bundleNum int, expectedHash string) (int64, error) {
	url := fmt.Sprintf("%s/data/%d", strings.TrimSuffix(baseURL, "/"), bundleNum)
	filename := fmt.Sprintf("%06d.jsonl.zst", bundleNum)
	filepath := filepath.Join(c.bundleDir, filename)

	// Create request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, err
	}

	// Download
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Write to temp file
	tempPath := filepath + ".tmp"
	outFile, err := os.Create(tempPath)
	if err != nil {
		return 0, err
	}

	written, err := io.Copy(outFile, resp.Body)
	outFile.Close()

	if err != nil {
		os.Remove(tempPath)
		return 0, err
	}

	// Verify hash
	if expectedHash != "" {
		valid, actualHash, err := c.operations.VerifyHash(tempPath, expectedHash)
		if err != nil {
			os.Remove(tempPath)
			return 0, fmt.Errorf("hash verification failed: %w", err)
		}
		if !valid {
			os.Remove(tempPath)
			return 0, fmt.Errorf("hash mismatch: expected %s, got %s",
				expectedHash[:16]+"...", actualHash[:16]+"...")
		}
	}

	// Rename to final location
	if err := os.Rename(tempPath, filepath); err != nil {
		os.Remove(tempPath)
		return 0, err
	}

	return written, nil
}
