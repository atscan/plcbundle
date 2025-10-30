package plc

import (
	"time"

	"github.com/goccy/go-json"
)

// PLCOperation represents a single operation from the PLC directory
type PLCOperation struct {
	DID       string          `json:"did"`
	Operation json.RawMessage `json:"operation"` // Lazy
	CID       string          `json:"cid"`
	Nullified interface{}     `json:"nullified,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`

	RawJSON         []byte                 `json:"-"`
	ParsedOperation map[string]interface{} `json:"-"` // Pre-parsed cache
}

// IsNullified checks if this operation has been nullified
func (op *PLCOperation) IsNullified() bool {
	if op.Nullified == nil {
		return false
	}

	switch v := op.Nullified.(type) {
	case bool:
		return v
	case string:
		return v != ""
	default:
		return false
	}
}

// GetNullifyingCID returns the CID that nullified this operation
func (op *PLCOperation) GetNullifyingCID() string {
	if s, ok := op.Nullified.(string); ok {
		return s
	}
	return ""
}

// DIDDocument represents a DID document from PLC
type DIDDocument struct {
	Context            []string             `json:"@context"`
	ID                 string               `json:"id"`
	AlsoKnownAs        []string             `json:"alsoKnownAs"`
	VerificationMethod []VerificationMethod `json:"verificationMethod"`
	Service            []Service            `json:"service"`
}

// VerificationMethod represents a verification method in a DID document
type VerificationMethod struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	Controller         string `json:"controller"`
	PublicKeyMultibase string `json:"publicKeyMultibase"`
}

// Service represents a service endpoint in a DID document
type Service struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
}

// ExportOptions contains options for exporting PLC operations
type ExportOptions struct {
	Count int    // Number of operations to fetch
	After string // ISO 8601 datetime string to start after
}

// DIDHistoryEntry represents a single operation in DID history
type DIDHistoryEntry struct {
	Operation PLCOperation `json:"operation"`
	PLCBundle string       `json:"plc_bundle,omitempty"`
}

// DIDHistory represents the full history of a DID
type DIDHistory struct {
	DID        string            `json:"did"`
	Current    *PLCOperation     `json:"current"`
	Operations []DIDHistoryEntry `json:"operations"`
}

// EndpointInfo contains extracted endpoint information from an operation
type EndpointInfo struct {
	Type     string // "pds", "labeler", etc.
	Endpoint string
}

// GetOperationData parses Operation into map (with caching)
func (op *PLCOperation) GetOperationData() (map[string]interface{}, error) {
	// Return cached if already parsed
	if op.ParsedOperation != nil {
		return op.ParsedOperation, nil
	}

	// Parse on first call
	if len(op.Operation) == 0 {
		return nil, nil
	}

	var data map[string]interface{}
	if err := json.UnmarshalNoEscape(op.Operation, &data); err != nil {
		return nil, err
	}

	// Cache it
	op.ParsedOperation = data

	return data, nil
}

// GetOperationMap is an alias for compatibility
func (op *PLCOperation) GetOperationMap() (map[string]interface{}, error) {
	return op.GetOperationData()
}
