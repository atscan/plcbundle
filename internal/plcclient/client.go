package plcclient

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/goccy/go-json"
)

// Client is a client for the PLC directory
type Client struct {
	baseURL     string
	httpClient  *http.Client
	rateLimiter *RateLimiter
	logger      Logger
	userAgent   string
}

// Logger is a simple logging interface
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// defaultLogger uses standard log package
type defaultLogger struct{}

func (d defaultLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (d defaultLogger) Println(v ...interface{}) {
	log.Println(v...)
}

// ClientOption is a functional option for configuring the Client
type ClientOption func(*Client)

// WithLogger sets a custom logger
func WithLogger(logger Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

// WithUserAgent sets a custom user agent string
func WithUserAgent(userAgent string) ClientOption {
	return func(c *Client) {
		c.userAgent = userAgent
	}
}

// WithTimeout sets a custom HTTP timeout
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// WithRateLimit sets a custom rate limit (requests per period)
func WithRateLimit(requestsPerPeriod int, period time.Duration) ClientOption {
	return func(c *Client) {
		if c.rateLimiter != nil {
			c.rateLimiter.Stop()
		}
		c.rateLimiter = NewRateLimiter(requestsPerPeriod, period)
	}
}

// NewClient creates a new PLC directory client
// Default: 90 requests per minute, 60 second timeout
func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		rateLimiter: NewRateLimiter(90, time.Minute),
		logger:      defaultLogger{},
		userAgent:   "plcbundle/dev",
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Close closes the client and cleans up resources
func (c *Client) Close() {
	if c.rateLimiter != nil {
		c.rateLimiter.Stop()
	}
}

// Export fetches export data from PLC directory with rate limiting and retry
func (c *Client) Export(ctx context.Context, opts ExportOptions) ([]PLCOperation, error) {
	return c.exportWithRetry(ctx, opts, 5)
}

// exportWithRetry implements retry logic with exponential backoff for rate limits
func (c *Client) exportWithRetry(ctx context.Context, opts ExportOptions, maxRetries int) ([]PLCOperation, error) {
	var lastErr error
	backoff := 1 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Wait for rate limiter token
		if err := c.rateLimiter.Wait(ctx); err != nil {
			return nil, err
		}

		operations, retryAfter, err := c.doExport(ctx, opts)

		if err == nil {
			return operations, nil
		}

		lastErr = err

		// Check if it's a rate limit error (429)
		if retryAfter > 0 {
			c.logger.Printf("Rate limited by PLC directory, waiting %v before retry %d/%d",
				retryAfter, attempt, maxRetries)

			select {
			case <-time.After(retryAfter):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// Other errors - exponential backoff
		if attempt < maxRetries {
			c.logger.Printf("Request failed (attempt %d/%d): %v, retrying in %v",
				attempt, maxRetries, err, backoff)

			select {
			case <-time.After(backoff):
				backoff *= 2 // Exponential backoff
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// doExport performs the actual HTTP request
func (c *Client) doExport(ctx context.Context, opts ExportOptions) ([]PLCOperation, time.Duration, error) {
	url := fmt.Sprintf("%s/export", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", c.userAgent)

	// Add query parameters
	q := req.URL.Query()
	if opts.Count > 0 {
		q.Add("count", fmt.Sprintf("%d", opts.Count))
	}
	if opts.After != "" {
		q.Add("after", opts.After)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Handle rate limiting (429)
	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := parseRetryAfter(resp)
		return nil, retryAfter, fmt.Errorf("rate limited (429)")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var operations []PLCOperation

	// PLC export returns newline-delimited JSON
	scanner := bufio.NewScanner(resp.Body)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	lineCount := 0
	for scanner.Scan() {
		lineCount++
		line := scanner.Bytes()

		if len(line) == 0 {
			continue
		}

		var op PLCOperation
		if err := json.Unmarshal(line, &op); err != nil {
			c.logger.Printf("Warning: failed to parse operation on line %d: %v", lineCount, err)
			continue
		}

		// CRITICAL: Store the original raw JSON bytes
		op.RawJSON = make([]byte, len(line))
		copy(op.RawJSON, line)

		operations = append(operations, op)
	}

	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("error reading response: %w", err)
	}

	return operations, 0, nil
}

// parseRetryAfter parses the Retry-After header
func parseRetryAfter(resp *http.Response) time.Duration {
	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter == "" {
		// Default to 5 minutes if no header
		return 5 * time.Minute
	}

	// Try parsing as seconds
	if seconds, err := strconv.Atoi(retryAfter); err == nil {
		return time.Duration(seconds) * time.Second
	}

	// Try parsing as HTTP date
	if t, err := http.ParseTime(retryAfter); err == nil {
		return time.Until(t)
	}

	// Default
	return 5 * time.Minute
}

// GetDID fetches a specific DID document from PLC
func (c *Client) GetDID(ctx context.Context, did string) (*DIDDocument, error) {
	// Wait for rate limiter
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/%s", c.baseURL, did)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := parseRetryAfter(resp)
		return nil, fmt.Errorf("rate limited, retry after %v", retryAfter)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var doc DIDDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, err
	}

	return &doc, nil
}

// GetStats returns basic stats about the client
func (c *Client) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"base_url": c.baseURL,
		"timeout":  c.httpClient.Timeout,
	}
}

// GetBaseURL returns the PLC directory base URL
func (c *Client) GetBaseURL() string {
	return c.baseURL
}
