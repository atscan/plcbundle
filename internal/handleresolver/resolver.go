package handleresolver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

// Client resolves AT Protocol handles to DIDs via XRPC
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new handle resolver client
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// ResolveHandle resolves a handle to a DID using com.atproto.identity.resolveHandle
func (c *Client) ResolveHandle(ctx context.Context, handle string) (string, error) {
	// Validate handle format
	if err := ValidateHandleFormat(handle); err != nil {
		return "", err
	}

	// Build XRPC URL
	endpoint := fmt.Sprintf("%s/xrpc/com.atproto.identity.resolveHandle", c.baseURL)

	// Add query parameter
	params := url.Values{}
	params.Add("handle", handle)
	fullURL := fmt.Sprintf("%s?%s", endpoint, params.Encode())

	// Create request
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to resolve handle: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("resolver returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var result struct {
		DID string `json:"did"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	if result.DID == "" {
		return "", fmt.Errorf("resolver returned empty DID")
	}

	// Validate returned DID
	if !strings.HasPrefix(result.DID, "did:plc:") && !strings.HasPrefix(result.DID, "did:web:") {
		return "", fmt.Errorf("invalid DID format returned: %s", result.DID)
	}

	return result.DID, nil
}

// ValidateHandleFormat validates AT Protocol handle format
func ValidateHandleFormat(handle string) error {
	if handle == "" {
		return fmt.Errorf("handle cannot be empty")
	}

	// Handle can't be a DID
	if strings.HasPrefix(handle, "did:") {
		return fmt.Errorf("input is already a DID, not a handle")
	}

	// Basic length check
	if len(handle) > 253 {
		return fmt.Errorf("handle too long (max 253 chars)")
	}

	// Must have at least one dot (domain.tld)
	if !strings.Contains(handle, ".") {
		return fmt.Errorf("handle must be a domain (e.g., user.bsky.social)")
	}

	// Valid handle pattern (simplified - matches AT Protocol spec)
	validPattern := regexp.MustCompile(`^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)
	if !validPattern.MatchString(handle) {
		return fmt.Errorf("invalid handle format")
	}

	return nil
}

// IsHandle checks if a string looks like a handle (not a DID)
func IsHandle(input string) bool {
	return !strings.HasPrefix(input, "did:")
}

// NormalizeHandle normalizes handle format (removes at:// prefix if present)
func NormalizeHandle(handle string) string {
	handle = strings.TrimPrefix(handle, "at://")
	handle = strings.TrimPrefix(handle, "@")
	return handle
}

// GetBaseURL returns the PLC directory base URL
func (c *Client) GetBaseURL() string {
	return c.baseURL
}
