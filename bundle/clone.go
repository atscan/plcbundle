package bundle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// CloneFromRemote clones bundles from a remote HTTP endpoint
func (m *Manager) CloneFromRemote(ctx context.Context, opts CloneOptions) (*CloneResult, error) {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.SaveInterval <= 0 {
		opts.SaveInterval = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = m.logger
	}

	result := &CloneResult{}
	startTime := time.Now()

	// Step 1: Fetch remote index
	opts.Logger.Printf("Fetching remote index from %s", opts.RemoteURL)
	remoteIndex, err := m.loadRemoteIndex(opts.RemoteURL)
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

	// Step 2: Determine which bundles to download
	localBundleMap := make(map[int]*BundleMetadata)
	for _, meta := range m.index.GetBundles() {
		localBundleMap[meta.BundleNumber] = meta
	}

	// Create map of remote metadata for easy lookup
	remoteBundleMap := make(map[int]*BundleMetadata)
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

	// Step 3: Set up periodic index saving (using remote metadata)
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
				// Save index using remote metadata for downloaded bundles
				downloadedMu.Lock()
				bundles := make([]int, len(downloadedBundles))
				copy(bundles, downloadedBundles)
				downloadedMu.Unlock()

				if opts.Verbose {
					opts.Logger.Printf("Periodic save: updating index with %d bundles", len(bundles))
				}
				m.updateIndexFromRemote(bundles, remoteBundleMap, false) // silent during periodic save
			}
		}
	}()

	// Step 4: Download bundles concurrently
	successList, failedList, bytes := m.downloadBundlesConcurrent(
		ctx,
		opts.RemoteURL,
		bundlesToDownload,
		remoteBundleMap, // Pass the metadata map for hash verification
		totalBytes,
		opts.Workers,
		opts.ProgressFunc,
		opts.Verbose,
	)

	result.Downloaded = len(successList)
	result.Failed = len(failedList)
	result.TotalBytes = bytes
	result.FailedBundles = failedList
	result.Interrupted = ctx.Err() != nil

	// Stop periodic saves
	saveCancel()
	<-saveDone

	// Step 5: Final index update using remote metadata
	opts.Logger.Printf("Updating local index...")
	if err := m.updateIndexFromRemote(successList, remoteBundleMap, opts.Verbose); err != nil {
		return result, fmt.Errorf("failed to update index: %w", err)
	}

	result.Duration = time.Since(startTime)
	return result, nil
}

// downloadBundlesConcurrent downloads bundles using a worker pool
func (m *Manager) downloadBundlesConcurrent(
	ctx context.Context,
	baseURL string,
	bundleNumbers []int,
	remoteBundleMap map[int]*BundleMetadata,
	totalBytes int64,
	workers int,
	progressFunc func(downloaded, total int, bytesDownloaded, bytesTotal int64),
	verbose bool,
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
	client := &http.Client{
		Timeout: 120 * time.Second,
	}

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

				// Download bundle with hash verification
				bytes, err := m.downloadBundle(client, baseURL, j.bundleNum, j.expectedHash)

				// Update progress
				mu.Lock()
				processedCount++
				if err == nil {
					processedBytes += bytes
					success = append(success, j.bundleNum)
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

	// Send jobs with expected hashes
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
			m.logger.Printf("Failed to download bundle %06d: %v", res.bundleNum, res.err)
		} else if res.success && verbose {
			m.logger.Printf("✓ Downloaded and verified bundle %06d (%d bytes)", res.bundleNum, res.bytes)
		}
	}

	mu.Lock()
	successList = success
	failedList = failed
	downloadedBytes = processedBytes
	mu.Unlock()

	return
}

// updateIndexFromRemote updates local index with metadata from remote index
func (m *Manager) updateIndexFromRemote(bundleNumbers []int, remoteMeta map[int]*BundleMetadata, verbose bool) error {
	if len(bundleNumbers) == 0 {
		return nil
	}

	// Add/update bundles in local index using remote metadata
	// Hash verification was already done during download
	for _, num := range bundleNumbers {
		if meta, exists := remoteMeta[num]; exists {
			// Verify the file exists locally
			path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", num))
			if !m.operations.FileExists(path) {
				m.logger.Printf("Warning: bundle %06d not found locally, skipping", num)
				continue
			}

			// Add to index (no need to re-verify hash - already verified during download)
			m.index.AddBundle(meta)

			if verbose {
				m.logger.Printf("Added bundle %06d to index", num)
			}
		}
	}

	// Save index
	return m.SaveIndex()
}

// loadRemoteIndex loads an index from a remote URL
func (m *Manager) loadRemoteIndex(baseURL string) (*Index, error) {
	indexURL := strings.TrimSuffix(baseURL, "/") + "/index.json"

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

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

	var idx Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

	return &idx, nil
}

// downloadBundle downloads a single bundle file and verifies its hash
func (m *Manager) downloadBundle(client *http.Client, baseURL string, bundleNum int, expectedHash string) (int64, error) {
	url := fmt.Sprintf("%s/data/%d", strings.TrimSuffix(baseURL, "/"), bundleNum)
	filename := fmt.Sprintf("%06d.jsonl.zst", bundleNum)
	filepath := filepath.Join(m.config.BundleDir, filename)

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

	// Write to temp file (atomic write)
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

	// Verify hash before committing
	if expectedHash != "" {
		valid, actualHash, err := m.operations.VerifyHash(tempPath, expectedHash)
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
