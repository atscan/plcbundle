package bundle

import (
	"encoding/json"
	"fmt"
	"time"

	didplc "github.com/did-method-plc/go-didplc"
	"tangled.org/atscan.net/plcbundle/plc"
)

// Validator validates PLC operations using go-didplc
type Validator struct {
	logger Logger
}

// InvalidOperation represents an operation that failed validation
type InvalidOperation struct {
	CID    string
	DID    string
	Reason string
}

// InvalidCallback is called immediately when an invalid operation is found
type InvalidCallback func(InvalidOperation)

// NewValidator creates a new validator
func NewValidator(logger Logger) *Validator {
	return &Validator{
		logger: logger,
	}
}

// ValidateBundleOperations validates all operations (simple version, returns error only)
func (v *Validator) ValidateBundleOperations(ops []plc.PLCOperation) error {
	_, err := v.ValidateBundleOperationsWithDetails(ops)
	return err
}

// ValidateBundleOperationsWithDetails validates and returns details about invalid operations
func (v *Validator) ValidateBundleOperationsWithDetails(ops []plc.PLCOperation) ([]InvalidOperation, error) {
	var invalid []InvalidOperation

	// Use streaming validation but collect results
	err := v.ValidateBundleOperationsStreaming(ops, func(inv InvalidOperation) {
		invalid = append(invalid, inv)
	})

	return invalid, err
}

// ValidateBundleOperationsStreaming validates and streams invalid operations via callback
func (v *Validator) ValidateBundleOperationsStreaming(ops []plc.PLCOperation, callback InvalidCallback) error {
	if len(ops) == 0 {
		return nil
	}

	// First pass: validate each operation individually and parse
	opsByDID := make(map[string][]didplc.LogEntry)
	opCIDMap := make(map[string]string) // CID -> DID mapping
	parseErrors := 0
	validationErrors := 0

	for _, op := range ops {
		opCIDMap[op.CID] = op.DID

		// Try to parse operation
		var opEnum didplc.OpEnum
		if err := parseOperationToEnum(op, &opEnum); err != nil {
			if callback != nil {
				callback(InvalidOperation{
					CID:    op.CID,
					DID:    op.DID,
					Reason: fmt.Sprintf("parse error: %v", err),
				})
			}
			parseErrors++
			continue
		}

		// Create log entry
		logEntry := didplc.LogEntry{
			DID:       op.DID,
			CID:       op.CID,
			CreatedAt: op.CreatedAt.Format(time.RFC3339Nano),
			Nullified: op.IsNullified(),
			Operation: opEnum,
		}

		// Validate individual entry (checks CID match, signature for genesis, etc.)
		if err := logEntry.Validate(); err != nil {
			if callback != nil {
				callback(InvalidOperation{
					CID:    op.CID,
					DID:    op.DID,
					Reason: fmt.Sprintf("validation error: %v", err),
				})
			}
			validationErrors++
			// Still add to chain for chain validation (some errors might be at chain level)
		}

		opsByDID[op.DID] = append(opsByDID[op.DID], logEntry)
	}

	// Second pass: validate chains (chronological order, nullification, etc.)
	chainErrors := 0
	for did, entries := range opsByDID {
		if err := didplc.VerifyOpLog(entries); err != nil {
			// Chain validation failed - report which specific operations are affected
			// Try to be more specific about which operations caused the failure
			errMsg := err.Error()

			if callback != nil {
				// For chain errors, report all operations in the chain
				// (we don't know which specific one caused it without more detailed analysis)
				for _, entry := range entries {
					callback(InvalidOperation{
						CID:    entry.CID,
						DID:    did,
						Reason: fmt.Sprintf("chain error: %v", errMsg),
					})
				}
			}
			chainErrors++
		}
	}

	totalErrors := parseErrors + validationErrors + chainErrors
	if totalErrors > 0 {
		return fmt.Errorf("%d parse errors, %d validation errors, %d chain errors",
			parseErrors, validationErrors, chainErrors)
	}

	return nil
}

// parseOperationToEnum converts plc.PLCOperation to didplc.OpEnum
func parseOperationToEnum(op plc.PLCOperation, opEnum *didplc.OpEnum) error {
	// Try to use RawJSON first for exact parsing
	if len(op.RawJSON) > 0 {
		// Extract just the operation part from the full record
		var fullRecord map[string]interface{}
		if err := json.Unmarshal(op.RawJSON, &fullRecord); err != nil {
			return fmt.Errorf("failed to unmarshal RawJSON: %w", err)
		}

		// Get the "operation" field
		if opData, ok := fullRecord["operation"]; ok {
			// Re-marshal just the operation data
			data, err := json.Marshal(opData)
			if err != nil {
				return fmt.Errorf("failed to marshal operation: %w", err)
			}

			if err := json.Unmarshal(data, opEnum); err != nil {
				return fmt.Errorf("failed to unmarshal into OpEnum: %w", err)
			}
			return nil
		}
	}

	// Fallback: use the Operation map
	data, err := json.Marshal(op.Operation)
	if err != nil {
		return fmt.Errorf("failed to marshal operation: %w", err)
	}

	if err := json.Unmarshal(data, opEnum); err != nil {
		return fmt.Errorf("failed to unmarshal into OpEnum: %w", err)
	}

	return nil
}
