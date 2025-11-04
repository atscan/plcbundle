package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// ProgressBar shows progress of an operation
type ProgressBar struct {
	total        int
	current      int
	totalBytes   int64
	currentBytes int64
	startTime    time.Time
	mu           sync.Mutex
	width        int
	lastPrint    time.Time
	showBytes    bool
}

// NewProgressBar creates a new progress bar
func NewProgressBar(total int) *ProgressBar {
	return &ProgressBar{
		total:        total,
		current:      0,
		totalBytes:   0,
		currentBytes: 0,
		startTime:    time.Now(),
		width:        40,
		lastPrint:    time.Now(),
		showBytes:    false,
	}
}

// NewProgressBarWithBytes creates a new progress bar that tracks bytes
func NewProgressBarWithBytes(total int, totalBytes int64) *ProgressBar {
	return &ProgressBar{
		total:        total,
		current:      0,
		totalBytes:   totalBytes,
		currentBytes: 0,
		startTime:    time.Now(),
		width:        40,
		lastPrint:    time.Now(),
		showBytes:    true,
	}
}

// Increment increases the progress by 1
func (pb *ProgressBar) Increment() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.current++
	pb.print()
}

// Set sets the current progress
func (pb *ProgressBar) Set(current int) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.current = current
	pb.print()
}

// SetWithBytes sets the current progress and bytes processed
func (pb *ProgressBar) SetWithBytes(current int, bytesProcessed int64) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.current = current
	pb.currentBytes = bytesProcessed
	pb.print()
}

// Finish completes the progress bar
func (pb *ProgressBar) Finish() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.current = pb.total
	pb.currentBytes = pb.totalBytes
	pb.print()
	fmt.Fprintf(os.Stderr, "\n")
}

// print renders the progress bar (must be called with lock held)
func (pb *ProgressBar) print() {
	// Rate limit updates (max 10 per second)
	if time.Since(pb.lastPrint) < 100*time.Millisecond && pb.current < pb.total {
		return
	}
	pb.lastPrint = time.Now()

	// Calculate percentage
	percent := float64(pb.current) / float64(pb.total) * 100
	if pb.total == 0 {
		percent = 0
	}

	// Calculate bar
	filled := int(float64(pb.width) * float64(pb.current) / float64(pb.total))
	if filled > pb.width {
		filled = pb.width
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", pb.width-filled)

	// Calculate speed and ETA
	elapsed := time.Since(pb.startTime)
	speed := float64(pb.current) / elapsed.Seconds()
	remaining := pb.total - pb.current
	var eta time.Duration
	if speed > 0 {
		eta = time.Duration(float64(remaining)/speed) * time.Second
	}

	// Show MB/s if bytes are being tracked (changed condition)
	if pb.showBytes && pb.currentBytes > 0 {
		// Calculate MB/s (using decimal units: 1 MB = 1,000,000 bytes)
		mbProcessed := float64(pb.currentBytes) / (1000 * 1000)
		mbPerSec := mbProcessed / elapsed.Seconds()

		fmt.Fprintf(os.Stderr, "\r  [%s] %6.2f%% | %d/%d bundles | %.1f/s | %.1f MB/s | ETA: %s ",
			bar,
			percent,
			pb.current,
			pb.total,
			speed,
			mbPerSec,
			formatETA(eta))
	} else {
		fmt.Fprintf(os.Stderr, "\r  [%s] %6.2f%% | %d/%d bundles | %.1f/s | ETA: %s ",
			bar,
			percent,
			pb.current,
			pb.total,
			speed,
			formatETA(eta))
	}
}

// formatETA formats the ETA duration
func formatETA(d time.Duration) string {
	if d == 0 {
		return "calculating..."
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}
