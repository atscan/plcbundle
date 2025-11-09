package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/didindex"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	internalsync "tangled.org/atscan.net/plcbundle/internal/sync"
)

// BundleManager interface (for testing/mocking)
type BundleManager interface {
	Close()
	GetIndex() *bundleindex.Index
	LoadBundle(ctx context.Context, bundleNumber int) (*bundle.Bundle, error)
	VerifyBundle(ctx context.Context, bundleNumber int) (*bundle.VerificationResult, error)
	VerifyChain(ctx context.Context) (*bundle.ChainVerificationResult, error)
	GetInfo() map[string]interface{}
	GetMempoolStats() map[string]interface{}
	GetMempoolOperations() ([]plcclient.PLCOperation, error)
	ValidateMempool() error
	RefreshMempool() error
	ClearMempool() error
	FetchNextBundle(ctx context.Context, quiet bool) (*bundle.Bundle, error)
	SaveBundle(ctx context.Context, b *bundle.Bundle, quiet bool) (time.Duration, error) // ✨ Updated signature
	GetDIDIndexStats() map[string]interface{}
	GetDIDIndex() *didindex.Manager
	BuildDIDIndex(ctx context.Context, progress func(int, int)) error
	GetDIDOperationsWithLocations(ctx context.Context, did string, verbose bool) ([]bundle.PLCOperationWithLocation, error)
	GetDIDOperationsFromMempool(did string) ([]plcclient.PLCOperation, error)
	GetLatestDIDOperation(ctx context.Context, did string) (*plcclient.PLCOperation, error)
	LoadOperation(ctx context.Context, bundleNum, position int) (*plcclient.PLCOperation, error)
	CloneFromRemote(ctx context.Context, opts internalsync.CloneOptions) (*internalsync.CloneResult, error)
	ResolveDID(ctx context.Context, did string) (*bundle.ResolveDIDResult, error)
}

// PLCOperationWithLocation wraps operation with location info
type PLCOperationWithLocation = bundle.PLCOperationWithLocation

// getManager creates or opens a bundle manager
func getManager(plcURL string) (*bundle.Manager, string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, "", err
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, "", fmt.Errorf("failed to create directory: %w", err)
	}

	config := bundle.DefaultConfig(dir)

	var client *plcclient.Client
	if plcURL != "" {
		client = plcclient.NewClient(plcURL)
	}

	mgr, err := bundle.NewManager(config, client)
	if err != nil {
		return nil, "", err
	}

	return mgr, dir, nil
}

// getManagerInDirectory creates manager in specific directory
func getManagerInDirectory(dir string, plcURL string) (*bundle.Manager, string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, "", fmt.Errorf("failed to create directory: %w", err)
	}

	config := bundle.DefaultConfig(dir)

	var client *plcclient.Client
	if plcURL != "" {
		client = plcclient.NewClient(plcURL)
	}

	mgr, err := bundle.NewManager(config, client)
	if err != nil {
		return nil, "", err
	}

	return mgr, dir, nil
}

// getManagerFromCommand creates manager using command flags
func getManagerFromCommand(cmd *cobra.Command, plcURL string) (*bundle.Manager, string, error) {
	// Get --dir flag from root command
	dir, _ := cmd.Root().PersistentFlags().GetString("dir")

	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return nil, "", err
		}
	}

	// Convert to absolute path
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, "", fmt.Errorf("invalid directory path: %w", err)
	}

	return getManagerInDirectory(absDir, plcURL)
}

// parseBundleRange parses bundle range string
func parseBundleRange(rangeStr string) (start, end int, err error) {
	if !strings.Contains(rangeStr, "-") {
		var num int
		_, err = fmt.Sscanf(rangeStr, "%d", &num)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid bundle number: %w", err)
		}
		return num, num, nil
	}

	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format (expected: N or start-end)")
	}

	_, err = fmt.Sscanf(parts[0], "%d", &start)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start: %w", err)
	}

	_, err = fmt.Sscanf(parts[1], "%d", &end)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end: %w", err)
	}

	if start > end {
		return 0, 0, fmt.Errorf("start must be <= end")
	}

	return start, end, nil
}

// Formatting helpers

func formatBytes(bytes int64) string {
	const unit = 1000
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0f seconds", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1f minutes", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1f hours", d.Hours())
	}
	days := d.Hours() / 24
	if days < 30 {
		return fmt.Sprintf("%.1f days", days)
	}
	if days < 365 {
		return fmt.Sprintf("%.1f months", days/30)
	}
	return fmt.Sprintf("%.1f years", days/365)
}

func formatNumber(n int) string {
	s := fmt.Sprintf("%d", n)
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

// commandLogger adapts to types.Logger
type commandLogger struct {
	quiet bool
}

func (l *commandLogger) Printf(format string, v ...interface{}) {
	if !l.quiet {
		fmt.Fprintf(os.Stderr, format+"\n", v...)
	}
}

func (l *commandLogger) Println(v ...interface{}) {
	if !l.quiet {
		fmt.Fprintln(os.Stderr, v...)
	}
}
