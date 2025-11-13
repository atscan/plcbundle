package ui

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
	autoBytes    bool // Auto-calculate bytes from items
	bytesPerItem int64
}

// NewProgressBar creates a simple progress bar
func NewProgressBar(total int) *ProgressBar {
	return &ProgressBar{
		total:     total,
		startTime: time.Now(),
		width:     40,
		lastPrint: time.Now(),
		showBytes: false,
	}
}

// NewProgressBarWithBytes creates a progress bar that tracks bytes
func NewProgressBarWithBytes(total int, totalBytes int64) *ProgressBar {
	return &ProgressBar{
		total:      total,
		totalBytes: totalBytes,
		startTime:  time.Now(),
		width:      40,
		lastPrint:  time.Now(),
		showBytes:  true,
	}
}

// NewProgressBarWithBytesAuto creates a progress bar that auto-estimates bytes
// avgBytesPerItem is the estimated bytes per item (e.g., avg bundle size)
func NewProgressBarWithBytesAuto(total int, avgBytesPerItem int64) *ProgressBar {
	return &ProgressBar{
		total:        total,
		totalBytes:   int64(total) * avgBytesPerItem,
		startTime:    time.Now(),
		width:        40,
		lastPrint:    time.Now(),
		showBytes:    true,
		autoBytes:    true,
		bytesPerItem: avgBytesPerItem,
	}
}

// Set sets the current progress (auto-estimates bytes if enabled)
func (pb *ProgressBar) Set(current int) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.current = current

	// Auto-calculate bytes if enabled
	if pb.autoBytes && pb.bytesPerItem > 0 {
		pb.currentBytes = int64(current) * pb.bytesPerItem
	}

	pb.print()
}

// SetWithBytes sets progress with exact byte tracking
func (pb *ProgressBar) SetWithBytes(current int, bytesProcessed int64) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.current = current
	pb.currentBytes = bytesProcessed
	pb.showBytes = true
	pb.print()
}

// AddBytes increments current progress and adds bytes
func (pb *ProgressBar) AddBytes(increment int, bytes int64) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.current += increment
	pb.currentBytes += bytes
	pb.showBytes = true
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

// print renders the progress bar
func (pb *ProgressBar) print() {
	if time.Since(pb.lastPrint) < 100*time.Millisecond && pb.current < pb.total {
		return
	}
	pb.lastPrint = time.Now()

	percent := 0.0
	if pb.total > 0 {
		percent = float64(pb.current) / float64(pb.total) * 100
	}

	filled := 0
	if pb.total > 0 {
		filled = int(float64(pb.width) * float64(pb.current) / float64(pb.total))
		if filled > pb.width {
			filled = pb.width
		}
	}

	bar := strings.Repeat("█", filled) + strings.Repeat("░", pb.width-filled)

	elapsed := time.Since(pb.startTime)
	speed := 0.0
	if elapsed.Seconds() > 0 {
		speed = float64(pb.current) / elapsed.Seconds()
	}

	remaining := pb.total - pb.current
	var eta time.Duration
	if speed > 0 && remaining > 0 {
		eta = time.Duration(float64(remaining)/speed) * time.Second
	}

	isComplete := pb.current >= pb.total

	if pb.showBytes && pb.currentBytes > 0 {
		mbProcessed := float64(pb.currentBytes) / (1000 * 1000)
		mbPerSec := 0.0
		if elapsed.Seconds() > 0 {
			mbPerSec = mbProcessed / elapsed.Seconds()
		}

		if isComplete {
			fmt.Fprintf(os.Stderr, "\r  [%s] %6.2f%% | %d/%d | %.1f/s | %.1f MB/s | Done    ",
				bar, percent, pb.current, pb.total, speed, mbPerSec)
		} else {
			fmt.Fprintf(os.Stderr, "\r  [%s] %6.2f%% | %d/%d | %.1f/s | %.1f MB/s | ETA: %s ",
				bar, percent, pb.current, pb.total, speed, mbPerSec, formatETA(eta))
		}
	} else {
		if isComplete {
			fmt.Fprintf(os.Stderr, "\r  [%s] %6.2f%% | %d/%d | %.1f/s | Done    ",
				bar, percent, pb.current, pb.total, speed)
		} else {
			fmt.Fprintf(os.Stderr, "\r  [%s] %6.2f%% | %d/%d | %.1f/s | ETA: %s ",
				bar, percent, pb.current, pb.total, speed, formatETA(eta))
		}
	}
}

func formatETA(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}
