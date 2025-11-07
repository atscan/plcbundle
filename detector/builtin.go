// detector/builtin.go
package detector

import (
	"context"
	"regexp"
	"strings"

	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

// NoOpDetector is an empty detector for speed testing
type NoOpDetector struct{}

func NewNoOpDetector() *NoOpDetector {
	return &NoOpDetector{}
}

func (d *NoOpDetector) Name() string { return "noop" }
func (d *NoOpDetector) Description() string {
	return "Empty detector for benchmarking (always returns no match)"
}
func (d *NoOpDetector) Version() string { return "1.0.0" }

func (d *NoOpDetector) Detect(ctx context.Context, op plcclient.PLCOperation) (*Match, error) {
	// Instant return - no work done
	return nil, nil
}

// InvalidHandleDetector detects operations with invalid handle patterns
type InvalidHandleDetector struct {
	// Valid handle regex based on AT Protocol handle specification
	validHandlePattern *regexp.Regexp
}

func NewInvalidHandleDetector() *InvalidHandleDetector {
	return &InvalidHandleDetector{
		// Valid handle pattern: domain segments + TLD
		// Each segment: alphanumeric start/end, hyphens allowed in middle, max 63 chars per segment
		// TLD must start with letter
		validHandlePattern: regexp.MustCompile(`^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`),
	}
}

func (d *InvalidHandleDetector) Name() string { return "invalid_handle" }
func (d *InvalidHandleDetector) Description() string {
	return "Detects operations with invalid handle patterns (underscores, invalid chars, malformed)"
}
func (d *InvalidHandleDetector) Version() string { return "1.0.0" }

func (d *InvalidHandleDetector) Detect(ctx context.Context, op plcclient.PLCOperation) (*Match, error) {
	// Parse Operation field on-demand
	operation, err := op.GetOperationMap()
	if err != nil {
		return nil, err
	}
	if operation == nil {
		return nil, nil
	}

	if alsoKnownAs, ok := operation["alsoKnownAs"].([]interface{}); ok {

		for _, aka := range alsoKnownAs {
			if str, ok := aka.(string); ok {
				// Check if it's an at:// handle
				if !strings.HasPrefix(str, "at://") {
					continue
				}

				// Extract handle (remove at:// prefix)
				handle := strings.TrimPrefix(str, "at://")

				// Remove any path component (e.g., at://user.bsky.social/profile -> user.bsky.social)
				if idx := strings.Index(handle, "/"); idx > 0 {
					handle = handle[:idx]
				}

				// Check for underscore (invalid in Bluesky handles)
				if strings.Contains(handle, "_") {
					return &Match{
						Reason:     "underscore_in_handle",
						Category:   "invalid_handle",
						Confidence: 0.99,
						Note:       "Handle contains underscore which is invalid in AT Protocol",
						Metadata: map[string]interface{}{
							"invalid_handle": str,
							"extracted":      handle,
							"violation":      "underscore_character",
						},
					}, nil
				}

				// Check for other invalid characters (anything not alphanumeric, hyphen, or dot)
				invalidChars := regexp.MustCompile(`[^a-zA-Z0-9.-]`)
				if invalidChars.MatchString(handle) {
					return &Match{
						Reason:     "invalid_characters",
						Category:   "invalid_handle",
						Confidence: 0.99,
						Note:       "Handle contains invalid characters",
						Metadata: map[string]interface{}{
							"invalid_handle": str,
							"extracted":      handle,
							"violation":      "invalid_characters",
						},
					}, nil
				}

				// Check if handle matches valid AT Protocol pattern
				if !d.validHandlePattern.MatchString(handle) {
					return &Match{
						Reason:     "invalid_handle_pattern",
						Category:   "invalid_handle",
						Confidence: 0.95,
						Note:       "Handle does not match valid AT Protocol handle pattern",
						Metadata: map[string]interface{}{
							"invalid_handle": str,
							"extracted":      handle,
							"violation":      "pattern_mismatch",
						},
					}, nil
				}

				// Additional checks: handle length
				if len(handle) > 253 { // DNS maximum
					return &Match{
						Reason:     "handle_too_long",
						Category:   "invalid_handle",
						Confidence: 0.98,
						Note:       "Handle exceeds maximum length (253 characters)",
						Metadata: map[string]interface{}{
							"invalid_handle": str,
							"extracted":      handle,
							"length":         len(handle),
							"violation":      "exceeds_max_length",
						},
					}, nil
				}

				// Check segment lengths (each part between dots should be max 63 chars)
				segments := strings.Split(handle, ".")
				for i, segment := range segments {
					if len(segment) == 0 {
						return &Match{
							Reason:     "empty_segment",
							Category:   "invalid_handle",
							Confidence: 0.99,
							Note:       "Handle contains empty segment (consecutive dots)",
							Metadata: map[string]interface{}{
								"invalid_handle": str,
								"extracted":      handle,
								"violation":      "empty_segment",
							},
						}, nil
					}
					if len(segment) > 63 {
						return &Match{
							Reason:     "segment_too_long",
							Category:   "invalid_handle",
							Confidence: 0.98,
							Note:       "Handle segment exceeds maximum length (63 characters)",
							Metadata: map[string]interface{}{
								"invalid_handle": str,
								"extracted":      handle,
								"segment":        i,
								"segment_value":  segment,
								"length":         len(segment),
								"violation":      "segment_exceeds_max_length",
							},
						}, nil
					}
				}

				// Check minimum segments (at least 2: subdomain.tld)
				if len(segments) < 2 {
					return &Match{
						Reason:     "insufficient_segments",
						Category:   "invalid_handle",
						Confidence: 0.99,
						Note:       "Handle must have at least 2 segments (subdomain.tld)",
						Metadata: map[string]interface{}{
							"invalid_handle": str,
							"extracted":      handle,
							"segments":       len(segments),
							"violation":      "insufficient_segments",
						},
					}, nil
				}
			}
		}
	}

	return nil, nil
}

// AlsoKnownAsSpamDetector detects excessive/garbage alsoKnownAs entries
type AlsoKnownAsSpamDetector struct {
	maxLegitimateEntries int
	minGarbageLength     int
}

func NewAlsoKnownAsSpamDetector() *AlsoKnownAsSpamDetector {
	return &AlsoKnownAsSpamDetector{
		maxLegitimateEntries: 3,   // Normal operations have 1-3 entries
		minGarbageLength:     100, // Garbage strings are very long
	}
}

func (d *AlsoKnownAsSpamDetector) Name() string { return "aka_spam" }
func (d *AlsoKnownAsSpamDetector) Description() string {
	return "Detects spam through excessive or garbage alsoKnownAs entries"
}
func (d *AlsoKnownAsSpamDetector) Version() string { return "1.0.0" }

func (d *AlsoKnownAsSpamDetector) Detect(ctx context.Context, op plcclient.PLCOperation) (*Match, error) {
	// Parse Operation field on-demand
	operation, err := op.GetOperationMap()
	if err != nil {
		return nil, err
	}
	if operation == nil {
		return nil, nil
	}
	if alsoKnownAs, ok := operation["alsoKnownAs"].([]interface{}); ok {
		entryCount := len(alsoKnownAs)

		// Count different types of entries
		atURICount := 0
		garbageCount := 0
		var garbageExamples []string

		for _, aka := range alsoKnownAs {
			if str, ok := aka.(string); ok {
				if strings.HasPrefix(str, "at://") {
					atURICount++
				} else if len(str) > d.minGarbageLength {
					garbageCount++
					if len(garbageExamples) < 2 {
						// Store first few for evidence
						preview := str
						if len(preview) > 50 {
							preview = preview[:50] + "..."
						}
						garbageExamples = append(garbageExamples, preview)
					}
				}
			}
		}

		// Detection: Excessive entries
		if entryCount > d.maxLegitimateEntries {
			confidence := 0.80
			if garbageCount > 0 {
				confidence = 0.95 // Higher confidence if garbage detected
			}

			return &Match{
				Reason:     "excessive_aka_entries",
				Category:   "spam",
				Confidence: confidence,
				Note:       "Operation has excessive alsoKnownAs entries",
				Metadata: map[string]interface{}{
					"total_entries":    entryCount,
					"at_uri_count":     atURICount,
					"garbage_count":    garbageCount,
					"garbage_examples": garbageExamples,
				},
			}, nil
		}

		// Detection: Garbage entries present (even if count is low)
		if garbageCount > 0 {
			return &Match{
				Reason:     "garbage_aka_entries",
				Category:   "spam",
				Confidence: 0.98,
				Note:       "Operation contains garbage/random strings in alsoKnownAs",
				Metadata: map[string]interface{}{
					"total_entries":    entryCount,
					"garbage_count":    garbageCount,
					"garbage_examples": garbageExamples,
				},
			}, nil
		}
	}

	return nil, nil
}

// SpamPDSDetector detects known spam PDS endpoints
type SpamPDSDetector struct {
	spamEndpoints map[string]bool
	spamDomains   map[string]bool
}

func NewSpamPDSDetector() *SpamPDSDetector {
	return &SpamPDSDetector{
		spamEndpoints: map[string]bool{
			"pds.trump.com": true,
			// Add more as discovered
		},
		spamDomains: map[string]bool{
			"trump.com":        true,
			"donald.trump.com": true,
			// Add more as discovered
		},
	}
}

func (d *SpamPDSDetector) Name() string { return "spam_pds" }
func (d *SpamPDSDetector) Description() string {
	return "Detects operations using known spam PDS endpoints and fake domain claims"
}
func (d *SpamPDSDetector) Version() string { return "1.0.0" }

func (d *SpamPDSDetector) Detect(ctx context.Context, op plcclient.PLCOperation) (*Match, error) {
	// Parse Operation field on-demand
	operation, err := op.GetOperationMap()
	if err != nil {
		return nil, err
	}
	if operation == nil {
		return nil, nil
	}
	// Check PDS endpoint
	if services, ok := operation["services"].(map[string]interface{}); ok {
		if pds, ok := services["atproto_pds"].(map[string]interface{}); ok {
			if endpoint, ok := pds["endpoint"].(string); ok {
				host := extractHost(endpoint)

				// Check if it's a known spam PDS
				if d.spamEndpoints[host] {
					return &Match{
						Reason:     "spam_pds_endpoint",
						Category:   "spam",
						Confidence: 0.99,
						Note:       "Operation uses known spam PDS endpoint",
						Metadata: map[string]interface{}{
							"endpoint": endpoint,
							"host":     host,
						},
					}, nil
				}
			}
		}
	}

	// Check for spam domain claims in alsoKnownAs
	if alsoKnownAs, ok := operation["alsoKnownAs"].([]interface{}); ok {
		for _, aka := range alsoKnownAs {
			if str, ok := aka.(string); ok {
				if !strings.HasPrefix(str, "at://") {
					continue
				}

				// Extract domain from at:// URI
				domain := strings.TrimPrefix(str, "at://")
				if idx := strings.Index(domain, "/"); idx > 0 {
					domain = domain[:idx]
				}

				// Check if claiming spam domain
				if d.spamDomains[domain] {
					return &Match{
						Reason:     "fake_domain_claim",
						Category:   "impersonation",
						Confidence: 0.99,
						Note:       "Operation claims known spam/fake domain",
						Metadata: map[string]interface{}{
							"claimed_domain": domain,
							"handle":         str,
						},
					}, nil
				}

				// Check for subdomain patterns (like jr.donald.trump.com)
				for spamDomain := range d.spamDomains {
					if strings.HasSuffix(domain, "."+spamDomain) || domain == spamDomain {
						return &Match{
							Reason:     "fake_domain_claim",
							Category:   "impersonation",
							Confidence: 0.99,
							Note:       "Operation claims domain related to known spam domain",
							Metadata: map[string]interface{}{
								"claimed_domain": domain,
								"spam_domain":    spamDomain,
							},
						}, nil
					}
				}
			}
		}
	}

	return nil, nil
}

// ServiceAbuseDetector detects operations with abused service structures
type ServiceAbuseDetector struct {
	maxServiceTypeLength int
	maxEndpointLength    int
	maxHandleLength      int
}

func NewServiceAbuseDetector() *ServiceAbuseDetector {
	return &ServiceAbuseDetector{
		maxServiceTypeLength: 100, // Normal types are short (e.g., "AtprotoPersonalDataServer")
		maxEndpointLength:    200, // Normal endpoints are reasonable URLs
		maxHandleLength:      100, // Normal handles are short
	}
}

func (d *ServiceAbuseDetector) Name() string { return "service_abuse" }
func (d *ServiceAbuseDetector) Description() string {
	return "Detects operations with abused service structures (random strings, numeric keys)"
}
func (d *ServiceAbuseDetector) Version() string { return "1.0.0" }

func (d *ServiceAbuseDetector) Detect(ctx context.Context, op plcclient.PLCOperation) (*Match, error) {
	// Parse Operation field on-demand
	operation, err := op.GetOperationMap()
	if err != nil {
		return nil, err
	}
	if operation == nil {
		return nil, nil
	}
	if services, ok := operation["services"].(map[string]interface{}); ok {
		// Check for numeric service keys (spam uses "0", "1", "2" instead of proper names)
		hasNumericKeys := false
		numericKeyCount := 0

		for key := range services {
			// Check if key is a digit
			if len(key) == 1 && key >= "0" && key <= "9" {
				hasNumericKeys = true
				numericKeyCount++
			}
		}

		if hasNumericKeys && numericKeyCount > 1 {
			return &Match{
				Reason:     "numeric_service_keys",
				Category:   "service_abuse",
				Confidence: 0.98,
				Note:       "Services use numeric keys instead of proper names",
				Metadata: map[string]interface{}{
					"numeric_key_count": numericKeyCount,
				},
			}, nil
		}

		// Check each service for abuse patterns
		for serviceName, serviceData := range services {
			if serviceMap, ok := serviceData.(map[string]interface{}); ok {
				// Check service type length
				if serviceType, ok := serviceMap["type"].(string); ok {
					if len(serviceType) > d.maxServiceTypeLength {
						return &Match{
							Reason:     "excessive_service_type_length",
							Category:   "service_abuse",
							Confidence: 0.99,
							Note:       "Service type field contains excessively long random string",
							Metadata: map[string]interface{}{
								"service_name": serviceName,
								"type_length":  len(serviceType),
								"type_preview": serviceType[:50] + "...",
							},
						}, nil
					}
				}

				// Check endpoint length
				if endpoint, ok := serviceMap["endpoint"].(string); ok {
					if len(endpoint) > d.maxEndpointLength {
						return &Match{
							Reason:     "excessive_endpoint_length",
							Category:   "service_abuse",
							Confidence: 0.99,
							Note:       "Service endpoint contains excessively long random string",
							Metadata: map[string]interface{}{
								"service_name":     serviceName,
								"endpoint_length":  len(endpoint),
								"endpoint_preview": endpoint[:min(100, len(endpoint))] + "...",
							},
						}, nil
					}
				}
			}
		}
	}

	// Check for excessively long handles in alsoKnownAs
	if alsoKnownAs, ok := operation["alsoKnownAs"].([]interface{}); ok {
		for _, aka := range alsoKnownAs {
			if str, ok := aka.(string); ok {
				if strings.HasPrefix(str, "at://") {
					handle := strings.TrimPrefix(str, "at://")
					if len(handle) > d.maxHandleLength {
						return &Match{
							Reason:     "excessive_handle_length",
							Category:   "service_abuse",
							Confidence: 0.98,
							Note:       "Handle contains excessively long random string",
							Metadata: map[string]interface{}{
								"handle_length":  len(handle),
								"handle_preview": handle[:min(50, len(handle))] + "...",
							},
						}, nil
					}
				}
			}
		}
	}

	// Check for empty verificationMethods (common in this spam)
	if vm, ok := operation["verificationMethods"].(map[string]interface{}); ok {
		if len(vm) == 0 {
			// Empty verificationMethods alone isn't enough, but combined with other signals...
			// Check if there are other suspicious signals
			if services, ok := operation["services"].(map[string]interface{}); ok {
				if len(services) > 2 {
					// Multiple services + empty verificationMethods = suspicious
					return &Match{
						Reason:     "empty_verification_methods",
						Category:   "service_abuse",
						Confidence: 0.85,
						Note:       "Empty verificationMethods with multiple services",
						Metadata: map[string]interface{}{
							"service_count": len(services),
						},
					}, nil
				}
			}
		}
	}

	return nil, nil
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Helper functions

func extractHost(endpoint string) string {
	// Extract host from URL
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")
	if idx := strings.Index(endpoint, "/"); idx > 0 {
		endpoint = endpoint[:idx]
	}
	if idx := strings.Index(endpoint, ":"); idx > 0 {
		endpoint = endpoint[:idx]
	}
	return endpoint
}
