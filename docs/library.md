# Library Guide

A practical guide to using plcbundle as a Go library in your applications.

## Table of Contents

- [Getting Started](#getting-started)
- [Core Concepts](#core-concepts)
- [Common Patterns](#common-patterns)
- [Building Applications](#building-applications)
- [Advanced Usage](#advanced-usage)
- [Best Practices](#best-practices)
- [API Reference](#api-reference)

---

## Getting Started

### Installation

```bash
go get tangled.org/atscan.net/plcbundle
```

### Your First Program

Create a simple program to fetch and display bundle information:

```go
package main

import (
    "context"
    "log"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

func main() {
    // Create a manager
    mgr, err := plcbundle.New("./plc_data", "https://plc.directory")
    if err != nil {
        log.Fatal(err)
    }
    defer mgr.Close()
    
    // Get repository info
    info := mgr.GetInfo()
    log.Printf("Bundle directory: %s", info["bundle_dir"])
    
    // Get index stats
    index := mgr.GetIndex()
    stats := index.GetStats()
    log.Printf("Total bundles: %d", stats["bundle_count"])
}
```

Run it:
```bash
go run main.go
# 2025/01/15 10:30:00 Bundle directory: ./plc_data
# 2025/01/15 10:30:00 Total bundles: 0
```

### Fetching Your First Bundle

Let's fetch a bundle from the PLC directory:

```go
package main

import (
    "context"
    "log"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

func main() {
    mgr, err := plcbundle.New("./plc_data", "https://plc.directory")
    if err != nil {
        log.Fatal(err)
    }
    defer mgr.Close()
    
    ctx := context.Background()
    
    // Fetch next bundle
    log.Println("Fetching bundle...")
    bundle, err := mgr.FetchNext(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("✓ Fetched bundle %d", bundle.BundleNumber)
    log.Printf("  Operations: %d", len(bundle.Operations))
    log.Printf("  Unique DIDs: %d", bundle.DIDCount)
    log.Printf("  Time range: %s to %s", 
        bundle.StartTime.Format("2006-01-02"),
        bundle.EndTime.Format("2006-01-02"))
}
```

**What's happening here?**

1. `plcbundle.New()` creates a manager that handles all bundle operations
2. `FetchNext()` automatically:
   - Fetches operations from PLC directory
   - Creates a bundle when 10,000 operations are collected
   - Saves the bundle to disk
   - Updates the index
   - Returns the bundle object

### Reading Bundles

Once you have bundles, you can load and read them:

```go
package main

import (
    "context"
    "log"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

func main() {
    mgr, err := plcbundle.New("./plc_data", "")
    if err != nil {
        log.Fatal(err)
    }
    defer mgr.Close()
    
    ctx := context.Background()
    
    // Load bundle 1
    bundle, err := mgr.Load(ctx, 1)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Bundle %d loaded", bundle.BundleNumber)
    
    // Iterate through operations
    for i, op := range bundle.Operations {
        if i >= 5 {
            break // Just show first 5
        }
        log.Printf("%d. DID: %s, CID: %s", i+1, op.DID, op.CID)
    }
}
```

---

## Core Concepts

### The Manager

The `Manager` is your main entry point. It handles:
- Bundle storage and retrieval
- Index management
- PLC directory synchronization
- Verification
- Mempool management

**Creating a manager:**

```go
// Simple creation
mgr, err := plcbundle.New("./bundles", "https://plc.directory")

// Custom configuration
config := plcbundle.DefaultConfig("./bundles")
config.VerifyOnLoad = true
config.AutoRebuild = true

plcClient := plcbundle.NewPLCClient("https://plc.directory")
mgr, err := plcbundle.NewManager(config, plcClient)
```

### Bundles

A bundle contains exactly 10,000 operations:

```go
type Bundle struct {
    BundleNumber     int                    // Sequential number (1, 2, 3...)
    StartTime        time.Time              // First operation timestamp
    EndTime          time.Time              // Last operation timestamp
    Operations       []plcclient.PLCOperation     // The 10,000 operations
    DIDCount         int                    // Unique DIDs in bundle
    Hash             string                 // Chain hash (includes history)
    ContentHash      string                 // This bundle's content hash
    Parent           string                 // Previous bundle's chain hash
    CompressedSize   int64                  // File size on disk
    UncompressedSize int64                  // Original JSONL size
}
```

### The Index

The index tracks all bundles and their metadata:

```go
index := mgr.GetIndex()

// Get all bundles
bundles := index.GetBundles()
for _, meta := range bundles {
    log.Printf("Bundle %d: %s to %s", 
        meta.BundleNumber, 
        meta.StartTime.Format("2006-01-02"),
        meta.EndTime.Format("2006-01-02"))
}

// Get specific bundle metadata
meta, err := index.GetBundle(42)

// Get last bundle
lastBundle := index.GetLastBundle()
```

### Operations

Each operation represents a DID PLC directory event:

```go
type PLCOperation struct {
    DID       string          // The DID (did:plc:...)
    Operation json.RawMessage // Raw JSON bytes (use GetOperationMap() to parse)
    CID       string          // Content identifier
    Nullified interface{}     // nil, false, or CID string
    CreatedAt time.Time       // When it was created
    
    // Internal fields (populated automatically)
    RawJSON         []byte                 // Original JSON line
    ParsedOperation map[string]interface{} // Cached parsed data
}

// Accessing operation data:
operation, err := op.GetOperationMap()  // Parses Operation field (cached)
if err != nil || operation == nil {
    return
}

// Now you can access fields
services := operation["services"].(map[string]interface{})

// Check if operation was nullified
if op.IsNullified() {
    log.Printf("Operation %s was nullified by %s", op.CID, op.GetNullifyingCID())
}
```

### Accessing Operation Data

The `Operation` field uses lazy parsing for performance. Always parse it before accessing:

```go
// ❌ Wrong - won't compile
services := op.Operation["services"]

// ✅ Correct
operation, err := op.GetOperationMap()
if err != nil || operation == nil {
    return
}
services, ok := operation["services"].(map[string]interface{})
```

The parsed data is cached, so repeated calls are fast:
// First call: parses JSON
data1, _ := op.GetOperationMap()

// Second call: returns cached data (fast)
data2, _ := op.GetOperationMap()

---

## Common Patterns

### Pattern 1: Transparent Sync Service

**Goal:** Keep a local PLC mirror continuously synchronized.

This is the most common use case - maintaining an up-to-date copy of the PLC directory.

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

type SyncService struct {
    mgr      *plcbundle.Manager
    interval time.Duration
    stop     chan struct{}
}

func NewSyncService(bundleDir string, interval time.Duration) (*SyncService, error) {
    mgr, err := plcbundle.New(bundleDir, "https://plc.directory")
    if err != nil {
        return nil, err
    }
    
    return &SyncService{
        mgr:      mgr,
        interval: interval,
        stop:     make(chan struct{}),
    }, nil
}

func (s *SyncService) Start() {
    log.Println("Starting sync service...")
    
    // Initial sync
    s.sync()
    
    // Periodic sync
    ticker := time.NewTicker(s.interval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            s.sync()
        case <-s.stop:
            log.Println("Sync service stopped")
            return
        }
    }
}

func (s *SyncService) sync() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
    defer cancel()
    
    log.Println("Checking for new bundles...")
    
    fetched := 0
    for {
        bundle, err := s.mgr.FetchNext(ctx)
        if err != nil {
            if isInsufficientOps(err) {
                if fetched > 0 {
                    log.Printf("✓ Synced %d new bundles", fetched)
                } else {
                    log.Println("✓ Up to date")
                }
                return
            }
            log.Printf("Error: %v", err)
            return
        }
        
        fetched++
        log.Printf("✓ Fetched bundle %d (%d ops, %d DIDs)", 
            bundle.BundleNumber, len(bundle.Operations), bundle.DIDCount)
    }
}

func (s *SyncService) Stop() {
    close(s.stop)
    s.mgr.Close()
}

func isInsufficientOps(err error) bool {
    return err != nil && 
        (strings.Contains(err.Error(), "insufficient operations") ||
         strings.Contains(err.Error(), "no more available"))
}

func main() {
    service, err := NewSyncService("./plc_data", 5*time.Minute)
    if err != nil {
        log.Fatal(err)
    }
    
    // Start service in background
    go service.Start()
    
    // Wait for interrupt
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
    <-sigChan
    
    log.Println("Shutting down...")
    service.Stop()
}
```

**Usage:**
```bash
go run main.go
# Starting sync service...
# Checking for new bundles...
# ✓ Fetched bundle 8548 (10000 ops, 8234 DIDs)
# ✓ Fetched bundle 8549 (10000 ops, 8156 DIDs)
# ✓ Up to date
# ... (repeats every 5 minutes)
```

### Pattern 2: Reading and Processing Operations

**Goal:** Process all historical operations for analysis.

```go
package main

import (
    "context"
    "log"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

type OperationProcessor struct {
    mgr *plcbundle.Manager
}

func NewOperationProcessor(bundleDir string) (*OperationProcessor, error) {
    mgr, err := plcbundle.New(bundleDir, "")
    if err != nil {
        return nil, err
    }
    
    return &OperationProcessor{mgr: mgr}, nil
}

func (p *OperationProcessor) ProcessAll() error {
    ctx := context.Background()
    
    index := p.mgr.GetIndex()
    bundles := index.GetBundles()
    
    log.Printf("Processing %d bundles...", len(bundles))
    
    totalOps := 0
    uniqueDIDs := make(map[string]bool)
    
    for _, meta := range bundles {
        // Load bundle
        bundle, err := p.mgr.Load(ctx, meta.BundleNumber)
        if err != nil {
            return err
        }
        
        // Process operations
        for _, op := range bundle.Operations {
            totalOps++
            uniqueDIDs[op.DID] = true
            
            // Your processing logic here
            p.processOperation(op)
        }
        
        if meta.BundleNumber % 100 == 0 {
            log.Printf("Processed bundle %d...", meta.BundleNumber)
        }
    }
    
    log.Printf("✓ Processed %d operations from %d unique DIDs", 
        totalOps, len(uniqueDIDs))
    
    return nil
}

func (p *OperationProcessor) processOperation(op plcbundle.PLCOperation) {
    // Parse Operation field on-demand
    operation, err := op.GetOperationMap()
    if err != nil || operation == nil {
        return
    }
    
    // Example: Extract PDS endpoints
    if services, ok := operation["services"].(map[string]interface{}); ok {
        if pds, ok := services["atproto_pds"].(map[string]interface{}); ok {
            if endpoint, ok := pds["endpoint"].(string); ok {
                log.Printf("DID %s uses PDS: %s", op.DID, endpoint)
            }
        }
    }
}


func main() {
    processor, err := NewOperationProcessor("./plc_data")
    if err != nil {
        log.Fatal(err)
    }
    
    if err := processor.ProcessAll(); err != nil {
        log.Fatal(err)
    }
}
```

### Pattern 3: Time-Based Queries

**Goal:** Export operations from a specific time period.

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "time"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

func exportOperationsSince(bundleDir string, since time.Time, limit int) error {
    mgr, err := plcbundle.New(bundleDir, "")
    if err != nil {
        return err
    }
    defer mgr.Close()
    
    ctx := context.Background()
    
    // Export operations after timestamp
    ops, err := mgr.Export(ctx, since, limit)
    if err != nil {
        return err
    }
    
    log.Printf("Exporting %d operations...", len(ops))
    
    // Write as JSONL to stdout
    encoder := json.NewEncoder(os.Stdout)
    for _, op := range ops {
        if err := encoder.Encode(op); err != nil {
            return err
        }
    }
    
    return nil
}

func main() {
    // Export operations from the last 7 days
    since := time.Now().AddDate(0, 0, -7)
    
    if err := exportOperationsSince("./plc_data", since, 50000); err != nil {
        log.Fatal(err)
    }
}
```

**Output to file:**
```bash
go run main.go > last_7_days.jsonl
```

### Pattern 4: Verification Service

**Goal:** Periodically verify bundle integrity.

```go
package main

import (
    "context"
    "log"
    "time"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

type VerificationService struct {
    mgr      *plcbundle.Manager
    interval time.Duration
}

func NewVerificationService(bundleDir string, interval time.Duration) (*VerificationService, error) {
    mgr, err := plcbundle.New(bundleDir, "")
    if err != nil {
        return nil, err
    }
    
    return &VerificationService{
        mgr:      mgr,
        interval: interval,
    }, nil
}

func (v *VerificationService) Start() {
    ticker := time.NewTicker(v.interval)
    defer ticker.Stop()
    
    // Verify immediately on start
    v.verify()
    
    for range ticker.C {
        v.verify()
    }
}

func (v *VerificationService) verify() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
    defer cancel()
    
    log.Println("Starting chain verification...")
    start := time.Now()
    
    result, err := v.mgr.VerifyChain(ctx)
    if err != nil {
        log.Printf("❌ Verification error: %v", err)
        return
    }
    
    elapsed := time.Since(start)
    
    if result.Valid {
        log.Printf("✅ Chain verified: %d bundles, took %s", 
            result.ChainLength, elapsed.Round(time.Second))
        
        // Get head hash
        index := v.mgr.GetIndex()
        if last := index.GetLastBundle(); last != nil {
            log.Printf("   Head hash: %s...", last.Hash[:16])
        }
    } else {
        log.Printf("❌ Chain broken at bundle %d: %s", 
            result.BrokenAt, result.Error)
        
        // Alert or take action
        v.handleBrokenChain(result)
    }
}

func (v *VerificationService) handleBrokenChain(result *plcbundle.ChainVerificationResult) {
    // Send alert, trigger re-sync, etc.
    log.Printf("⚠️  ALERT: Chain integrity compromised!")
    // TODO: Implement your alerting logic
}

func main() {
    service, err := NewVerificationService("./plc_data", 24*time.Hour)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Println("Verification service started (daily checks)")
    service.Start()
}
```

### Pattern 5: Custom HTTP API

**Goal:** Build a custom API on top of your bundle archive.

```go
package main

import (
    "encoding/json"
    "log"
    "net/http"
    "strconv"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

type API struct {
    mgr *plcbundle.Manager
}

func NewAPI(bundleDir string) (*API, error) {
    mgr, err := plcbundle.New(bundleDir, "")
    if err != nil {
        return nil, err
    }
    
    return &API{mgr: mgr}, nil
}

func (api *API) handleStats(w http.ResponseWriter, r *http.Request) {
    index := api.mgr.GetIndex()
    stats := index.GetStats()
    
    response := map[string]interface{}{
        "bundles":     stats["bundle_count"],
        "first":       stats["first_bundle"],
        "last":        stats["last_bundle"],
        "total_size":  stats["total_size"],
        "start_time":  stats["start_time"],
        "end_time":    stats["end_time"],
        "updated_at":  stats["updated_at"],
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

func (api *API) handleOperations(w http.ResponseWriter, r *http.Request) {
    bundleNumStr := r.URL.Query().Get("bundle")
    if bundleNumStr == "" {
        http.Error(w, "bundle parameter required", http.StatusBadRequest)
        return
    }
    
    bundleNum, err := strconv.Atoi(bundleNumStr)
    if err != nil {
        http.Error(w, "invalid bundle number", http.StatusBadRequest)
        return
    }
    
    ctx := r.Context()
    bundle, err := api.mgr.Load(ctx, bundleNum)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }
    
    w.Header().Set("Content-Type", "application/x-ndjson")
    encoder := json.NewEncoder(w)
    for _, op := range bundle.Operations {
        encoder.Encode(op)
    }
}

func (api *API) handleDID(w http.ResponseWriter, r *http.Request) {
    did := r.URL.Query().Get("did")
    if did == "" {
        http.Error(w, "did parameter required", http.StatusBadRequest)
        return
    }
    
    ctx := r.Context()
    
    // Search through bundles for this DID
    var operations []plcbundle.PLCOperation
    
    index := api.mgr.GetIndex()
    bundles := index.GetBundles()
    
    for _, meta := range bundles {
        bundle, err := api.mgr.Load(ctx, meta.BundleNumber)
        if err != nil {
            continue
        }
        
        for _, op := range bundle.Operations {
            if op.DID == did {
                operations = append(operations, op)
            }
        }
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "did":        did,
        "operations": operations,
        "count":      len(operations),
    })
}

func main() {
    api, err := NewAPI("./plc_data")
    if err != nil {
        log.Fatal(err)
    }
    
    http.HandleFunc("/stats", api.handleStats)
    http.HandleFunc("/operations", api.handleOperations)
    http.HandleFunc("/did", api.handleDID)
    
    log.Println("API listening on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

**Usage:**
```bash
# Get stats
curl http://localhost:8080/stats

# Get operations from bundle 1
curl http://localhost:8080/operations?bundle=1

# Get all operations for a DID
curl http://localhost:8080/did?did=did:plc:example123
```

---

## Building Applications

### Application 1: PDS Discovery Tool

Find all PDS endpoints in the network:

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

type PDSTracker struct {
    mgr       *plcbundle.Manager
    endpoints map[string]int  // endpoint -> count
}

func NewPDSTracker(bundleDir string) (*PDSTracker, error) {
    mgr, err := plcbundle.New(bundleDir, "")
    if err != nil {
        return nil, err
    }
    
    return &PDSTracker{
        mgr:       mgr,
        endpoints: make(map[string]int),
    }, nil
}

func (pt *PDSTracker) Scan() error {
    ctx := context.Background()
    
    index := pt.mgr.GetIndex()
    bundles := index.GetBundles()
    
    log.Printf("Scanning %d bundles for PDS endpoints...", len(bundles))
    
    for _, meta := range bundles {
        bundle, err := pt.mgr.Load(ctx, meta.BundleNumber)
        if err != nil {
            return err
        }
        
        for _, op := range bundle.Operations {
            if endpoint := pt.extractPDS(op); endpoint != "" {
                pt.endpoints[endpoint]++
            }
        }
    }
    
    return nil
}

func (pt *PDSTracker) extractPDS(op plcbundle.PLCOperation) string {
    // Parse Operation field on-demand
    operation, err := op.GetOperationMap()
    if err != nil || operation == nil {
        return ""
    }
    
    services, ok := operation["services"].(map[string]interface{})
    if !ok {
        return ""
    }
    
    pds, ok := services["atproto_pds"].(map[string]interface{})
    if !ok {
        return ""
    }
    
    endpoint, ok := pds["endpoint"].(string)
    if !ok {
        return ""
    }
    
    return endpoint
}


func (pt *PDSTracker) PrintResults() {
    log.Printf("\nFound %d unique PDS endpoints:\n", len(pt.endpoints))
    
    // Sort by count
    type endpointCount struct {
        endpoint string
        count    int
    }
    
    var sorted []endpointCount
    for endpoint, count := range pt.endpoints {
        sorted = append(sorted, endpointCount{endpoint, count})
    }
    
    sort.Slice(sorted, func(i, j int) bool {
        return sorted[i].count > sorted[j].count
    })
    
    // Print top 20
    for i, ec := range sorted {
        if i >= 20 {
            break
        }
        fmt.Printf("%3d. %s (%d DIDs)\n", i+1, ec.endpoint, ec.count)
    }
}

func main() {
    tracker, err := NewPDSTracker("./plc_data")
    if err != nil {
        log.Fatal(err)
    }
    
    if err := tracker.Scan(); err != nil {
        log.Fatal(err)
    }
    
    tracker.PrintResults()
}
```

### Application 2: DID History Viewer

View the complete history of a DID:

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

type DIDHistory struct {
    DID        string                    `json:"did"`
    Operations []plcbundle.PLCOperation  `json:"operations"`
    FirstSeen  time.Time                 `json:"first_seen"`
    LastSeen   time.Time                 `json:"last_seen"`
    OpCount    int                       `json:"operation_count"`
}

func getDIDHistory(bundleDir, did string) (*DIDHistory, error) {
    mgr, err := plcbundle.New(bundleDir, "")
    if err != nil {
        return nil, err
    }
    defer mgr.Close()
    
    ctx := context.Background()
    
    history := &DIDHistory{
        DID:        did,
        Operations: make([]plcbundle.PLCOperation, 0),
    }
    
    index := mgr.GetIndex()
    bundles := index.GetBundles()
    
    log.Printf("Searching for DID %s...", did)
    
    for _, meta := range bundles {
        bundle, err := mgr.Load(ctx, meta.BundleNumber)
        if err != nil {
            continue
        }
        
        for _, op := range bundle.Operations {
            if op.DID == did {
                history.Operations = append(history.Operations, op)
            }
        }
    }
    
    if len(history.Operations) == 0 {
        return nil, fmt.Errorf("DID not found")
    }
    
    // Set timestamps
    history.FirstSeen = history.Operations[0].CreatedAt
    history.LastSeen = history.Operations[len(history.Operations)-1].CreatedAt
    history.OpCount = len(history.Operations)
    
    return history, nil
}

func main() {
    if len(os.Args) < 2 {
        log.Fatal("Usage: did-history <did>")
    }
    
    did := os.Args[1]
    
    history, err := getDIDHistory("./plc_data", did)
    if err != nil {
        log.Fatal(err)
    }
    
    // Print as JSON
    encoder := json.NewEncoder(os.Stdout)
    encoder.SetIndent("", "  ")
    encoder.Encode(history)
}
```

### Application 3: Real-time Monitor

Monitor new operations as they arrive:

```go
package main

import (
    "context"
    "log"
    "time"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

type Monitor struct {
    mgr          *plcbundle.Manager
    lastSeen     int  // Last bundle number processed
    pollInterval time.Duration
}

func NewMonitor(bundleDir string, pollInterval time.Duration) (*Monitor, error) {
    mgr, err := plcbundle.New(bundleDir, "https://plc.directory")
    if err != nil {
        return nil, err
    }
    
    // Get current position
    index := mgr.GetIndex()
    lastBundle := index.GetLastBundle()
    lastSeen := 0
    if lastBundle != nil {
        lastSeen = lastBundle.BundleNumber
    }
    
    return &Monitor{
        mgr:          mgr,
        lastSeen:     lastSeen,
        pollInterval: pollInterval,
    }, nil
}

func (m *Monitor) Start() {
    log.Println("Monitor started, watching for new bundles...")
    
    ticker := time.NewTicker(m.pollInterval)
    defer ticker.Stop()
    
    for range ticker.C {
        m.check()
    }
}

func (m *Monitor) check() {
    ctx := context.Background()
    
    // Try to fetch next bundle
    bundle, err := m.mgr.FetchNext(ctx)
    if err != nil {
        // Not an error if no new bundle available
        return
    }
    
    // New bundle!
    log.Printf("🔔 New bundle: %d", bundle.BundleNumber)
    log.Printf("   Operations: %d", len(bundle.Operations))
    log.Printf("   DIDs: %d", bundle.DIDCount)
    log.Printf("   Time: %s", bundle.EndTime.Format("2006-01-02 15:04:05"))
    
    // Process new operations
    m.processNewOperations(bundle)
    
    m.lastSeen = bundle.BundleNumber
}

func (m *Monitor) processNewOperations(bundle *plcbundle.Bundle) {
    for _, op := range bundle.Operations {
        // Check for interesting operations
        if op.IsNullified() {
            log.Printf("   ⚠️  Nullified: %s", op.DID)
        }
        
        // Check for new DIDs (operation type "create")
        operation, err := op.GetOperationMap()
        if err == nil && operation != nil {
            if opType, ok := operation["type"].(string); ok && opType == "create" {
                log.Printf("   ➕ New DID: %s", op.DID)
            }
        }
    }
}

func main() {
    monitor, err := NewMonitor("./plc_data", 30*time.Second)
    if err != nil {
        log.Fatal(err)
    }
    
    monitor.Start()
}
```

---

## Advanced Usage

### Custom Configuration

Full control over bundle manager behavior:

```go
package main

import (
    "log"
    "runtime"
    "time"
    
    "tangled.org/atscan.net/plcbundle/bundle"
    "tangled.org/atscan.net/plcbundle/plcclient"
    plcbundle "tangled.org/atscan.net/plcbundle"
)

func main() {
    // Custom configuration
    config := &bundle.Config{
        BundleDir:       "./my_bundles",
        VerifyOnLoad:    true,                    // Verify hashes when loading
        AutoRebuild:     true,                    // Auto-rebuild index if needed
        RebuildWorkers:  runtime.NumCPU(),        // Parallel workers for rebuild
        Logger:          &MyCustomLogger{},       // Custom logger
        
        // Progress callback for rebuild
        RebuildProgress: func(current, total int) {
            if current%100 == 0 {
                log.Printf("Rebuild: %d/%d (%.1f%%)", 
                    current, total, float64(current)/float64(total)*100)
            }
        },
    }
    
    // Custom PLC client with rate limiting
    plcClient := plcclient.NewClient("https://plc.directory",
        plcclient.WithRateLimit(60, time.Minute),      // 60 req/min
        plcclient.WithTimeout(30*time.Second),         // 30s timeout
        plcclient.WithLogger(&MyCustomLogger{}),       // Custom logger
    )
    
    // Create manager
    mgr, err := bundle.NewManager(config, plcClient)
    if err != nil {
        log.Fatal(err)
    }
    defer mgr.Close()
    
    log.Println("Manager created with custom configuration")
}

// Custom logger implementation
type MyCustomLogger struct{}

func (l *MyCustomLogger) Printf(format string, v ...interface{}) {
    // Add custom formatting, filtering, etc.
    log.Printf("[PLCBUNDLE] "+format, v...)
}

func (l *MyCustomLogger) Println(v ...interface{}) {
    log.Println(append([]interface{}{"[PLCBUNDLE]"}, v...)...)
}
```

### Streaming Data

Stream bundle data without loading everything into memory:

```go
package main

import (
    "bufio"
    "context"
    "encoding/json"
    "io"
    "log"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

func streamBundle(mgr *plcbundle.Manager, bundleNumber int) error {
    ctx := context.Background()
    
    // Get decompressed stream
    reader, err := mgr.StreamDecompressed(ctx, bundleNumber)
    if err != nil {
        return err
    }
    defer reader.Close()
    
    // Read line by line (JSONL)
    scanner := bufio.NewScanner(reader)
    
    // Set buffer size for large lines
    buf := make([]byte, 0, 64*1024)
    scanner.Buffer(buf, 1024*1024)
    
    lineNum := 0
    for scanner.Scan() {
        lineNum++
        
        var op plcbundle.PLCOperation
        if err := json.Unmarshal(scanner.Bytes(), &op); err != nil {
            log.Printf("Warning: failed to parse line %d: %v", lineNum, err)
            continue
        }
        
        // Process operation without storing all in memory
        processOperation(op)
    }
    
    return scanner.Err()
}

func processOperation(op plcbundle.PLCOperation) {
    // Your processing logic
    log.Printf("Processing: %s", op.DID)
}

func main() {
    mgr, err := plcbundle.New("./plc_data", "")
    if err != nil {
        log.Fatal(err)
    }
    defer mgr.Close()
    
    // Stream bundle 1
    if err := streamBundle(mgr, 1); err != nil {
        log.Fatal(err)
    }
}
```

### Parallel Processing

Process multiple bundles concurrently:

```go
package main

import (
    "context"
    "log"
    "sync"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

func processParallel(mgr *plcbundle.Manager, workers int) error {
    ctx := context.Background()
    
    index := mgr.GetIndex()
    bundles := index.GetBundles()
    
    // Create job channel
    jobs := make(chan int, len(bundles))
    results := make(chan error, len(bundles))
    
    // Start workers
    var wg sync.WaitGroup
    for w := 0; w < workers; w++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for bundleNum := range jobs {
                if err := processBundle(ctx, mgr, bundleNum); err != nil {
                    results <- err
                } else {
                    results <- nil
                }
            }
        }()
    }
    
    // Send jobs
    for _, meta := range bundles {
        jobs <- meta.BundleNumber
    }
    close(jobs)
    
    // Wait for completion
    go func() {
        wg.Wait()
        close(results)
    }()
    
    // Collect results
    errors := 0
    for err := range results {
        if err != nil {
            log.Printf("Error: %v", err)
            errors++
        }
    }
    
    if errors > 0 {
        return fmt.Errorf("%d bundles failed processing", errors)
    }
    
    return nil
}

func processBundle(ctx context.Context, mgr *plcbundle.Manager, bundleNum int) error {
    bundle, err := mgr.Load(ctx, bundleNum)
    if err != nil {
        return err
    }
    
    // Process operations
    for _, op := range bundle.Operations {
        // Your logic here
        _ = op
    }
    
    log.Printf("Processed bundle %d", bundleNum)
    return nil
}

func main() {
    mgr, err := plcbundle.New("./plc_data", "")
    if err != nil {
        log.Fatal(err)
    }
    defer mgr.Close()
    
    // Process with 8 workers
    if err := processParallel(mgr, 8); err != nil {
        log.Fatal(err)
    }
}
```

### Working with Mempool

Access operations before they're bundled:

```go
package main

import (
    "log"
    
    plcbundle "tangled.org/atscan.net/plcbundle"
)

func main() {
    mgr, err := plcbundle.New("./plc_data", "https://plc.directory")
    if err != nil {
        log.Fatal(err)
    }
    defer mgr.Close()
    
    // Get mempool stats
    stats := mgr.GetMempoolStats()
    
    count := stats["count"].(int)
    targetBundle := stats["target_bundle"].(int)
    canCreate := stats["can_create_bundle"].(bool)
    
    log.Printf("Mempool status:")
    log.Printf("  Target bundle: %d", targetBundle)
    log.Printf("  Operations: %d/%d", count, plcbundle.BUNDLE_SIZE)
    log.Printf("  Ready: %v", canCreate)
    
    if count > 0 {
        // Get mempool operations
        ops, err := mgr.GetMempoolOperations()
        if err != nil {
            log.Fatal(err)
        }
        
        log.Printf("Latest unbundled operations:")
        for i, op := range ops {
            if i >= 5 {
                break
            }
            log.Printf("  %d. %s (%s)", i+1, op.DID, op.CreatedAt.Format("15:04:05"))
        }
    }
    
    // Validate chronological order
    if err := mgr.ValidateMempool(); err != nil {
        log.Printf("⚠️  Mempool validation failed: %v", err)
    } else {
        log.Println("✓ Mempool validated")
    }
}
```

---

## Best Practices

### 1. Always Close the Manager

Use `defer` to ensure cleanup:

```go
mgr, err := plcbundle.New("./plc_data", "https://plc.directory")
if err != nil {
    return err
}
defer mgr.Close()  // Always close!
```

### 2. Handle Context Cancellation

Support graceful shutdown:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Listen for interrupt
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

go func() {
    <-sigChan
    log.Println("Interrupt received, stopping...")
    cancel()
}()

// Use context in operations
bundle, err := mgr.FetchNext(ctx)
if err == context.Canceled {
    log.Println("Operation cancelled gracefully")
    return nil
}
```

### 3. Check Errors Properly

Distinguish between different error types:

```go
bundle, err := mgr.FetchNext(ctx)
if err != nil {
    // Check if it's just "caught up"
    if strings.Contains(err.Error(), "insufficient operations") {
        log.Println("No new bundles available (caught up)")
        return nil
    }
    
    // Real error
    return fmt.Errorf("fetch failed: %w", err)
}
```

### 4. Use Streaming for Large Datasets

Don't load everything into memory:

```go
// ❌ Bad: Loads all operations into memory
index := mgr.GetIndex()
var allOps []plcbundle.PLCOperation
for _, meta := range index.GetBundles() {
    bundle, _ := mgr.Load(ctx, meta.BundleNumber)
    allOps = append(allOps, bundle.Operations...)
}

// ✅ Good: Process one bundle at a time
for _, meta := range index.GetBundles() {
    bundle, _ := mgr.Load(ctx, meta.BundleNumber)
    for _, op := range bundle.Operations {
        processOperation(op)
    }
}
```

### 5. Enable Verification in Production

```go
config := plcbundle.DefaultConfig("./plc_data")
config.VerifyOnLoad = true  // Verify hashes when loading

mgr, err := plcbundle.NewManager(config, plcClient)
```

### 6. Log Appropriately

Implement custom logger for production:

```go
type ProductionLogger struct {
    logger *zap.Logger
}

func (l *ProductionLogger) Printf(format string, v ...interface{}) {
    l.logger.Sugar().Infof(format, v...)
}

func (l *ProductionLogger) Println(v ...interface{}) {
    l.logger.Sugar().Info(v...)
}
```

### 7. Handle Rate Limits

Configure PLC client appropriately:

```go
// Production: Be conservative
plcClient := plcclient.NewClient("https://plc.directory",
    plcclient.WithRateLimit(60, time.Minute),  // 60 req/min max
    plcclient.WithTimeout(60*time.Second),
)

// Development: Can be more aggressive (but respectful)
plcClient := plcclient.NewClient("https://plc.directory",
    plcclient.WithRateLimit(90, time.Minute),
    plcclient.WithTimeout(30*time.Second),
)
```

---

## API Reference

### Manager Methods

```go
// Creation
New(bundleDir, plcURL string) (*Manager, error)
NewManager(config *Config, plcClient *PLCClient) (*Manager, error)

// Lifecycle
Close()

// Fetching
FetchNext(ctx) (*Bundle, error)

// Loading
Load(ctx, bundleNumber int) (*Bundle, error)

// Verification
Verify(ctx, bundleNumber int) (*VerificationResult, error)
VerifyChain(ctx) (*ChainVerificationResult, error)

// Exporting
Export(ctx, afterTime time.Time, count int) ([]PLCOperation, error)

// Streaming
StreamRaw(ctx, bundleNumber int) (io.ReadCloser, error)
StreamDecompressed(ctx, bundleNumber int) (io.ReadCloser, error)

// Index
GetIndex() *Index
ScanBundle(path string, bundleNumber int) (*BundleMetadata, error)
Scan() (*DirectoryScanResult, error)

// Mempool
GetMempoolStats() map[string]interface{}
GetMempoolOperations() ([]PLCOperation, error)
ValidateMempool() error
ClearMempool() error

// Info
GetInfo() map[string]interface{}
IsBundleIndexed(bundleNumber int) bool
```

### Index Methods

```go
// Creation
NewIndex() *Index
LoadIndex(path string) (*Index, error)

// Persistence
Save(path string) error

// Queries
GetBundle(bundleNumber int) (*BundleMetadata, error)
GetLastBundle() *BundleMetadata
GetBundles() []*BundleMetadata
GetBundleRange(start, end int) []*BundleMetadata

// Stats
Count() int
FindGaps() []int
GetStats() map[string]interface{}
```

### Configuration Types

```go
type Config struct {
    BundleDir       string
    VerifyOnLoad    bool
    AutoRebuild     bool
    RebuildWorkers  int
    RebuildProgress func(current, total int)
    Logger          Logger
}

type Logger interface {
    Printf(format string, v ...interface{})
    Println(v ...interface{})
}
```

---

## Troubleshooting

### Bundle Not Found Error

```go
bundle, err := mgr.Load(ctx, 999)
if err != nil {
    if strings.Contains(err.Error(), "not in index") {
        // Bundle doesn't exist
        log.Printf("Bundle 999 hasn't been fetched yet")
    }
}
```

### Insufficient Operations Error

```go
bundle, err := mgr.FetchNext(ctx)
if err != nil {
    if strings.Contains(err.Error(), "insufficient operations") {
        // Not enough operations for a complete bundle
        // Check mempool
        stats := mgr.GetMempoolStats()
        count := stats["count"].(int)
        log.Printf("Only %d operations available (need %d)", count, plcbundle.BUNDLE_SIZE)
    }
}
```

### Memory Usage

If processing large numbers of bundles:

```go
// Force garbage collection between bundles
for _, meta := range index.GetBundles() {
    bundle, _ := mgr.Load(ctx, meta.BundleNumber)
    processBundle(bundle)
    
    runtime.GC()  // Help garbage collector
}
```

---

## Examples Repository

Find complete, runnable examples at:
- https://github.com/plcbundle/examples

Including:
- Complete sync service
- API server
- Analysis tools
- Monitoring services

