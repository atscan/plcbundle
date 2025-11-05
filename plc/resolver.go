package plc

import (
	"fmt"
	"strings"
)

// DIDState represents the current state of a DID (PLC-specific format)
type DIDState struct {
	DID                 string                       `json:"did"`
	RotationKeys        []string                     `json:"rotationKeys"`
	VerificationMethods map[string]string            `json:"verificationMethods"`
	AlsoKnownAs         []string                     `json:"alsoKnownAs"`
	Services            map[string]ServiceDefinition `json:"services"`
}

// ServiceDefinition represents a service endpoint
type ServiceDefinition struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

// AuditLogEntry represents a single entry in the audit log
type AuditLogEntry struct {
	DID       string      `json:"did"`
	Operation interface{} `json:"operation"` // The parsed operation data
	CID       string      `json:"cid"`
	Nullified interface{} `json:"nullified,omitempty"`
	CreatedAt string      `json:"createdAt"`
}

// ResolveDIDDocument constructs a DID document from operation log
// This is the main entry point for DID resolution
func ResolveDIDDocument(did string, operations []PLCOperation) (*DIDDocument, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations found for DID")
	}

	// Build current state from operations
	state, err := BuildDIDState(did, operations)
	if err != nil {
		return nil, err
	}

	// Convert to DID document format
	return StateToDIDDocument(state), nil
}

// BuildDIDState applies operations in order to build current DID state
func BuildDIDState(did string, operations []PLCOperation) (*DIDState, error) {
	var state *DIDState

	for _, op := range operations {
		// Skip nullified operations
		if op.IsNullified() {
			continue
		}

		// Parse operation data
		opData, err := op.GetOperationData()
		if err != nil {
			return nil, fmt.Errorf("failed to parse operation: %w", err)
		}

		if opData == nil {
			continue
		}

		// Check operation type
		opType, _ := opData["type"].(string)

		// Handle tombstone (deactivated DID)
		if opType == "plc_tombstone" {
			return nil, fmt.Errorf("DID has been deactivated")
		}

		// Initialize state on first operation
		if state == nil {
			state = &DIDState{
				DID:                 did,
				RotationKeys:        []string{},
				VerificationMethods: make(map[string]string),
				AlsoKnownAs:         []string{},
				Services:            make(map[string]ServiceDefinition),
			}
		}

		// Apply operation to state
		applyOperationToState(state, opData)
	}

	if state == nil {
		return nil, fmt.Errorf("no valid operations found")
	}

	return state, nil
}

// applyOperationToState updates state with data from an operation
func applyOperationToState(state *DIDState, opData map[string]interface{}) {
	// Update rotation keys
	if rotKeys, ok := opData["rotationKeys"].([]interface{}); ok {
		state.RotationKeys = make([]string, 0, len(rotKeys))
		for _, k := range rotKeys {
			if keyStr, ok := k.(string); ok {
				state.RotationKeys = append(state.RotationKeys, keyStr)
			}
		}
	}

	// Update verification methods
	if vm, ok := opData["verificationMethods"].(map[string]interface{}); ok {
		state.VerificationMethods = make(map[string]string)
		for key, val := range vm {
			if valStr, ok := val.(string); ok {
				state.VerificationMethods[key] = valStr
			}
		}
	}

	// Handle legacy signingKey format
	if signingKey, ok := opData["signingKey"].(string); ok {
		if state.VerificationMethods == nil {
			state.VerificationMethods = make(map[string]string)
		}
		state.VerificationMethods["atproto"] = signingKey
	}

	// Update alsoKnownAs
	if aka, ok := opData["alsoKnownAs"].([]interface{}); ok {
		state.AlsoKnownAs = make([]string, 0, len(aka))
		for _, a := range aka {
			if akaStr, ok := a.(string); ok {
				state.AlsoKnownAs = append(state.AlsoKnownAs, akaStr)
			}
		}
	}

	// Handle legacy handle format
	if handle, ok := opData["handle"].(string); ok {
		// Only set if alsoKnownAs is empty
		if len(state.AlsoKnownAs) == 0 {
			state.AlsoKnownAs = []string{"at://" + handle}
		}
	}

	// Update services
	if services, ok := opData["services"].(map[string]interface{}); ok {
		state.Services = make(map[string]ServiceDefinition)
		for key, svc := range services {
			if svcMap, ok := svc.(map[string]interface{}); ok {
				svcType, _ := svcMap["type"].(string)
				endpoint, _ := svcMap["endpoint"].(string)
				state.Services[key] = ServiceDefinition{
					Type:     svcType,
					Endpoint: normalizeServiceEndpoint(endpoint),
				}
			}
		}
	}

	// Handle legacy service format
	if service, ok := opData["service"].(string); ok {
		if state.Services == nil {
			state.Services = make(map[string]ServiceDefinition)
		}
		state.Services["atproto_pds"] = ServiceDefinition{
			Type:     "AtprotoPersonalDataServer",
			Endpoint: normalizeServiceEndpoint(service),
		}
	}
}

// StateToDIDDocument converts internal PLC state to W3C DID document format
func StateToDIDDocument(state *DIDState) *DIDDocument {
	// Base contexts - ALWAYS include multikey (matches PLC directory behavior)
	contexts := []string{
		"https://www.w3.org/ns/did/v1",
		"https://w3id.org/security/multikey/v1", // ← Always include this
	}

	hasSecp256k1 := false
	hasP256 := false

	// Check verification method key types for additional contexts
	for _, didKey := range state.VerificationMethods {
		keyType := detectKeyType(didKey)
		switch keyType {
		case "secp256k1":
			hasSecp256k1 = true
		case "p256":
			hasP256 = true
		}
	}

	// Add suite-specific contexts only if those key types are present
	if hasSecp256k1 {
		contexts = append(contexts, "https://w3id.org/security/suites/secp256k1-2019/v1")
	}
	if hasP256 {
		contexts = append(contexts, "https://w3id.org/security/suites/ecdsa-2019/v1")
	}

	doc := &DIDDocument{
		Context:            contexts,
		ID:                 state.DID,
		AlsoKnownAs:        []string{},             // ← Empty slice
		VerificationMethod: []VerificationMethod{}, // ← Empty slice
		Service:            []Service{},            // ← Empty slice
	}

	// Copy alsoKnownAs if present
	if len(state.AlsoKnownAs) > 0 {
		doc.AlsoKnownAs = state.AlsoKnownAs
	}

	// Convert services
	for id, svc := range state.Services {
		doc.Service = append(doc.Service, Service{
			ID:              "#" + id,
			Type:            svc.Type,
			ServiceEndpoint: svc.Endpoint,
		})
	}

	// Keep verification methods with full DID
	for id, didKey := range state.VerificationMethods {
		doc.VerificationMethod = append(doc.VerificationMethod, VerificationMethod{
			ID:                 state.DID + "#" + id,
			Type:               "Multikey",
			Controller:         state.DID,
			PublicKeyMultibase: ExtractMultibaseFromDIDKey(didKey),
		})
	}

	return doc
}

// detectKeyType detects the key type from did:key encoding
func detectKeyType(didKey string) string {
	multibase := ExtractMultibaseFromDIDKey(didKey)

	if len(multibase) < 3 {
		return "unknown"
	}

	// The 'z' is the base58btc multibase prefix
	// Actual key starts at position 1
	switch {
	case multibase[1] == 'Q' && multibase[2] == '3':
		return "secp256k1" // Starts with zQ3s
	case multibase[1] == 'D' && multibase[2] == 'n':
		return "p256" // Starts with zDn
	case multibase[1] == '6' && multibase[2] == 'M':
		return "ed25519" // Starts with z6Mk
	default:
		return "unknown"
	}
}

// ExtractMultibaseFromDIDKey extracts the multibase string from did:key: format
func ExtractMultibaseFromDIDKey(didKey string) string {
	return strings.TrimPrefix(didKey, "did:key:")
}

// ValidateDIDFormat validates did:plc format
func ValidateDIDFormat(did string) error {
	if !strings.HasPrefix(did, "did:plc:") {
		return fmt.Errorf("invalid DID method: must start with 'did:plc:'")
	}

	if len(did) != 32 {
		return fmt.Errorf("invalid DID length: expected 32 chars, got %d", len(did))
	}

	// Validate identifier part (24 chars, base32 alphabet)
	identifier := strings.TrimPrefix(did, "did:plc:")
	if len(identifier) != 24 {
		return fmt.Errorf("invalid identifier length: expected 24 chars, got %d", len(identifier))
	}

	// Check base32 alphabet (a-z, 2-7, no 0189)
	for _, c := range identifier {
		if !((c >= 'a' && c <= 'z') || (c >= '2' && c <= '7')) {
			return fmt.Errorf("invalid character in identifier: %c (must be base32: a-z, 2-7)", c)
		}
	}

	return nil
}

// FormatAuditLog formats operations as an audit log
func FormatAuditLog(operations []PLCOperation) []AuditLogEntry {
	log := make([]AuditLogEntry, 0, len(operations))

	for _, op := range operations {
		// Parse operation for the log
		opData, _ := op.GetOperationData()

		entry := AuditLogEntry{
			DID:       op.DID,
			Operation: opData,
			CID:       op.CID,
			Nullified: op.Nullified,
			CreatedAt: op.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		}

		log = append(log, entry)
	}

	return log
}

func normalizeServiceEndpoint(endpoint string) string {
	// If already has protocol, return as-is
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}

	// Legacy format: add https:// prefix
	return "https://" + endpoint
}
