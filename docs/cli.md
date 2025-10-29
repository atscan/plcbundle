# CLI Guide

A practical guide to using the `plcbundle` command-line tool.

## Table of Contents

- [Getting Started](#getting-started)
- [Basic Workflows](#basic-workflows)
- [Advanced Usage](#advanced-usage)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## Getting Started

### Installation

```bash
go install tangled.org/atscan.net/plcbundle/cmd/plcbundle@latest
```

Verify it's installed:
```bash
plcbundle version
# plcbundle version dev
```

### Your First Bundle

Let's fetch your first bundle from the PLC directory:

```bash
# Create a directory for your bundles
mkdir my-plc-archive
cd my-plc-archive

# Fetch one bundle
plcbundle fetch
```

You'll see output like:
```
Working in: /Users/you/my-plc-archive
Starting from bundle 000001
Fetching all available bundles...

Preparing bundle 000001 (mempool: 0 ops)...
Fetching more operations (have 0/10000)...
  Fetch #1: requesting 1000 operations (mempool: 0)
  Added 1000 new operations (mempool now: 1000)
  
... (continues fetching) ...

✓ Bundle 000001 ready (10000 ops, mempool: 0 remaining)
✓ Saved bundle 000001 (10000 operations, 8543 DIDs)

✓ Fetch complete: 1 bundles retrieved
```

**What just happened?**

plcbundle created two files:
- `000001.jsonl.zst` - Your first bundle (10,000 PLC operations, compressed)
- `index.json` - Index tracking bundle metadata and hashes

### Understanding the Files

**Bundle files** (`000001.jsonl.zst`):
- Contain exactly 10,000 operations each
- Compressed with zstd (~5x compression)
- Named with 6-digit zero-padding
- Immutable once created

**Index file** (`index.json`):
- Maps bundle numbers to metadata
- Contains cryptographic hashes
- Tracks the bundle chain
- Updated when bundles are added

**Mempool files** (`plc_mempool_*.jsonl`):
- Temporary staging for operations
- Auto-managed by plcbundle
- Safe to ignore (they clean up automatically)

### Check What You Have

```bash
plcbundle info
```

You'll see a summary:
```
═══════════════════════════════════════════════════════════
              PLC Bundle Repository Overview
═══════════════════════════════════════════════════════════

📊 Summary
   Bundles:       1
   Range:         000001 → 000001
   Compressed:    505 KB
   Operations:    10,000 records

🔐 Chain Hashes
   Root (bundle 000001):
     8f4e3a2d9c1b7f5e3a2d9c1b7f5e3a2d9c1b7f5e3a2d9c1b7f5e3a2d9c1b7f5e
```

---

## Basic Workflows

### Workflow 1: Building a Complete Archive

**Goal:** Download all historical PLC bundles to create a complete archive.

```bash
# Fetch all available bundles
plcbundle fetch -count 0
```

The `-count 0` means "fetch everything available". This will:
- Start from bundle 1 (or continue from where you left off)
- Keep fetching until caught up
- Create bundles of 10,000 operations each
- Stop when no more complete bundles can be formed

**Tip:** This can take a while. You can interrupt with Ctrl+C and resume later - just run the same command again.

### Workflow 2: Keeping Up-to-Date

**Goal:** Regularly sync new operations as they arrive.

```bash
# Run periodically (cron, systemd timer, etc.)
plcbundle fetch
```

Or use a simple script:
```bash
#!/bin/bash
# sync-plc.sh
cd /path/to/plc_data
plcbundle fetch
```

Run it every 5-10 minutes:
```bash
# Crontab
*/10 * * * * /path/to/sync-plc.sh
```

**What happens when there aren't enough operations?**

If fewer than 10,000 new operations exist, they're stored in the mempool:
```bash
plcbundle mempool
# Mempool Status:
#   Operations: 3,482
#   Progress: 34.8% (3482/10000)
#   Need 6,518 more operations
```

The bundle will be created automatically once 10,000 operations arrive.

### Workflow 3: Cloning from Another Server

**Goal:** Quickly sync bundles from an existing plcbundle server instead of fetching from PLC.

**Why?** Much faster! Downloading pre-made bundles is faster than fetching and bundling operations yourself.

```bash
# Clone from a public mirror
plcbundle clone https://plc.example.com
```

With progress tracking:
```
Cloning from: https://plc.example.com
Remote has 8,547 bundles
Downloading 8,547 bundles (4.2 GB)

  [████████░░░░░░] 68.2% | 5,829/8,547 | 42.5/s | 21.3 MB/s | ETA: 1m 4s
```

**Resume after interruption:**

Press Ctrl+C and the download stops gracefully:
```
⚠️  Interrupt received! Finishing current downloads and saving progress...

⚠️  Download interrupted by user

Results:
  Downloaded: 5,829
  Total size: 2.9 GB

✓ Progress saved. Re-run the clone command to resume.
```

Just run the same command again:
```bash
plcbundle clone https://plc.example.com
# Skips 5,829 existing bundles, downloads the rest
```

**Speed it up with more workers:**
```bash
plcbundle clone https://plc.example.com -workers 16
```

### Workflow 4: Verifying Your Archive

**Goal:** Ensure your bundles are intact and unmodified.

```bash
# Verify the entire chain
plcbundle verify
```

Output:
```
Verifying chain of 100 bundles...
  [ 10%] Verified 10/100 bundles...
  [ 50%] Verified 50/100 bundles...
  [100%] Verified 100/100 bundles...

✓ Chain is valid (100 bundles verified)
  Chain head: 8f4e3a2d9c1b7f5e...
```

**What's being verified?**

1. **File integrity**: Each bundle's compressed data matches its hash
2. **Chain integrity**: Each bundle correctly links to the previous one
3. **Complete chain**: No broken links from bundle 1 to the last

**Verify just one bundle:**
```bash
plcbundle verify -bundle 42
# ✓ Bundle 000042 is valid
```

### Workflow 5: Sharing Your Archive

**Goal:** Run an HTTP server so others can clone your bundles.

```bash
plcbundle serve
```

Output:
```
Starting plcbundle HTTP server...
  Directory: /Users/you/plc_data
  Listening: http://127.0.0.1:8080
  Sync mode: disabled
  Bundles available: 100

Press Ctrl+C to stop
```

Now others can clone from you:
```bash
# From another machine
plcbundle clone http://your-server:8080
```

**Serve on a different port:**
```bash
plcbundle serve -port 9000 -host 0.0.0.0
# Listening on all interfaces, port 9000
```

**Auto-sync while serving:**
```bash
plcbundle serve -sync -sync-interval 5m
# Automatically fetches new bundles every 5 minutes
```

---

## Advanced Usage

### Working with Mempool

The mempool is a staging area for operations waiting to form a complete bundle.

**Check mempool status:**
```bash
plcbundle mempool
```

Output:
```
Mempool Status:
  Target bundle: 000101
  Operations: 7,234
  Can create bundle: false (need 10000)
  Progress: 72.3% (7234/10000)
  [████████████████████████████░░░░░░░░░░░░]
  First operation: 2024-12-01 10:23:45
  Last operation: 2024-12-15 15:47:23
```

**Export mempool operations:**
```bash
plcbundle mempool -export > recent_operations.jsonl
# Exported 7,234 operations from mempool
```

**Clear mempool (use with caution):**
```bash
plcbundle mempool -clear
# ⚠ This will clear 7,234 operations from the mempool.
# Are you sure? [y/N]: y
# ✓ Mempool cleared
```

**When to clear mempool:**
- After corrupted operations
- When restarting from scratch
- During development/testing

**Validate mempool chronology:**
```bash
plcbundle mempool -validate
# ✓ Mempool validation passed
```

### Exporting Operations

**Goal:** Extract operations from bundles for analysis or processing.

**Export recent operations:**
```bash
plcbundle export -count 5000 > operations.jsonl
# Exported 5000 operations
```

**Export operations after a specific time:**
```bash
plcbundle export -after "2024-01-01T00:00:00Z" -count 10000 > jan_2024.jsonl
```

**Stream all operations (backfill):**
```bash
plcbundle backfill > all_operations.jsonl
# This streams ALL operations from all bundles
```

**Backfill specific range:**
```bash
plcbundle backfill -start 1 -end 100 > first_100_bundles.jsonl
```

### Rebuilding the Index

**Goal:** Regenerate `index.json` from bundle files.

**When you need this:**
- Downloaded bundle files manually
- Corrupted index file
- Migrated from another system
- After specification updates (during preview)

```bash
plcbundle rebuild
```

Output:
```
Rebuilding index from: /Users/you/plc_data
Using 8 workers
Found 100 bundle files

Processing bundles:
  [████████████████████████████████████████] 100% | 100/100 | 25.0/s

✓ Index rebuilt in 4.2s
  Total bundles: 100
  Compressed size: 50.5 MB
  Uncompressed size: 252.3 MB
  Average speed: 23.8 bundles/sec

✓ Chain verified: All bundles linked correctly
```

**Speed it up:**
```bash
plcbundle rebuild -workers 16
```

### Comparing with Remote

**Goal:** Check differences between your local archive and a remote server.

```bash
plcbundle compare https://plc.example.com
```

Output:
```
Comparison Results
══════════════════

Summary
───────
  Local bundles:      95
  Target bundles:     100
  Common bundles:     95
  Missing bundles:    5 ⚠️
  Hash mismatches:    0 ✓

Missing Bundles (in target but not local)
──────────────────────────────────────────
  000096
  000097
  000098
  000099
  000100

✗ Indexes have differences
```

**Auto-fetch missing bundles:**
```bash
plcbundle compare https://plc.example.com --fetch-missing
```

This will:
1. Compare indexes
2. Show differences
3. Download missing bundles
4. Update your index

### Serving with WebSocket Streaming

**Goal:** Provide real-time streaming of operations via WebSocket.

```bash
plcbundle serve -sync -websocket
```

Output:
```
Starting plcbundle HTTP server...
  Listening: http://127.0.0.1:8080
  Sync mode: ENABLED
  WebSocket: ENABLED (ws://127.0.0.1:8080/ws)
```

**Connect with websocat:**
```bash
# Stream all operations from the beginning
websocat ws://localhost:8080/ws

# Stream from cursor 100,000
websocat 'ws://localhost:8080/ws?cursor=100000'
```

**What's a cursor?**

Cursor = `(bundle_number * 10000) + position_in_bundle`

Example:
- Bundle 1, position 0 = cursor 0
- Bundle 1, position 500 = cursor 500
- Bundle 10, position 0 = cursor 90,000
- Bundle 10, position 2,345 = cursor 92,345

**Stream and process:**
```bash
websocat ws://localhost:8080/ws | jq 'select(.did | startswith("did:plc:"))'
# Filter operations in real-time with jq
```

---

## Best Practices

### 1. Regular Verification

Verify your archive periodically:
```bash
# Weekly cron job
0 0 * * 0 cd /path/to/plc_data && plcbundle verify
```

### 2. Backup Strategy

**Back up the index frequently:**
```bash
cp index.json index.json.backup
```

**Why?** The index is small but critical. Bundles can be rebuilt, but the index tracks everything.

**Back up bundles incrementally:**
```bash
# Rsync to backup server
rsync -av --progress *.jsonl.zst backup-server:/plc_archive/
```

### 3. Storage Planning

**Size estimates:**
- ~500 KB per bundle (compressed)
- 10,000 operations per bundle
- ~50 bytes per operation (compressed)

**Current PLC size** (check with):
```bash
curl https://plc.directory/export?count=1 | jq -r '.createdAt'
# Then estimate: ~10,000 bundles as of 2025 = ~5 GB
```

### 4. Monitoring

**Create a health check script:**
```bash
#!/bin/bash
# health-check.sh

cd /path/to/plc_data

# Check last bundle age
LAST_BUNDLE=$(plcbundle info | grep "Last Op" | cut -d: -f2-)
AGE=$(plcbundle info | grep "Age" | cut -d: -f2-)

echo "Last operation: $LAST_BUNDLE"
echo "Age: $AGE"

# Verify chain
if ! plcbundle verify -bundle $(plcbundle info | grep "Last bundle" | awk '{print $3}'); then
  echo "ERROR: Verification failed!"
  exit 1
fi

echo "Status: OK"
```

### 5. Network Efficiency

**Use clone instead of fetch when possible:**
```bash
# Slower: Fetch from PLC directory
plcbundle fetch

# Faster: Clone from mirror
plcbundle clone https://plc-mirror.example.com
```

**Rate limit considerations:**

The PLC directory rate limits API requests. plcbundle handles this automatically with:
- Exponential backoff
- Automatic retry
- ~90 requests/minute limit

### 6. Disk Space Management

**Check sizes:**
```bash
plcbundle info | grep -E "(Compressed|Bundles)"
# Bundles: 100
# Compressed: 50.5 MB
```

**Clean old mempools (safe):**
```bash
# Old mempools might linger if process was killed
rm plc_mempool_*.jsonl
plcbundle fetch  # Will recreate if needed
```

---

## Troubleshooting

### Bundle Count Mismatch

**Problem:**
```
Found 100 bundle files but index only has 95 entries
```

**Solution:**
```bash
plcbundle rebuild
```

This rescans all bundles and rebuilds the index.

---

### Hash Verification Failed

**Problem:**
```
✗ Bundle 000042 hash verification failed
  Expected hash: a1b2c3d4...
  Actual hash:   f6e5d4c3...
```

**Possible causes:**
1. Corrupted file during download
2. Bundle was modified
3. Downloaded from untrusted source

**Solution:**

If you have a trusted source:
```bash
# Re-download specific bundle
plcbundle clone https://trusted-mirror.com -workers 1
# (It will skip existing and re-download corrupt ones)
```

Or fetch fresh:
```bash
# Delete corrupted bundle
rm 000042.jsonl.zst

# Rebuild index (marks it as missing)
plcbundle rebuild

# Fetch it again
plcbundle fetch
```

---

### Chain Broken Error

**Problem:**
```
✗ Chain broken at bundle 000050
  Expected parent: a1b2c3d4...
  Actual parent:   f6e5d4c3...
```

**Meaning:** Bundle 50's parent hash doesn't match bundle 49's hash.

**Solution:**

This means bundles came from incompatible sources. You need a consistent chain:

```bash
# Start fresh or clone from one trusted source
rm *.jsonl.zst index.json
plcbundle clone https://trusted-mirror.com
```

---

### Out of Disk Space

**Problem:**
```
Error saving bundle: no space left on device
```

**Solution:**

Check space:
```bash
df -h .
```

Free up space or move to larger disk:
```bash
# Move to new location
mv /old/path/* /new/large/disk/
cd /new/large/disk/
plcbundle info  # Verify it works
```

---

### Fetch Stuck / No Progress

**Problem:** `plcbundle fetch` runs but doesn't create bundles.

**Check mempool:**
```bash
plcbundle mempool
# Operations: 3,482
# Need 6,518 more operations
```

**Meaning:** Not enough operations yet for a complete bundle.

**Solutions:**

1. **Wait** - More operations will arrive
2. **Check PLC connectivity:**
```bash
curl https://plc.directory/export?count=1
```

3. **Check rate limits:**
```bash
# Look for 429 errors in output
plcbundle fetch -count 1
```

---

### Port Already in Use

**Problem:**
```
Server error: listen tcp :8080: bind: address already in use
```

**Solution:**

Use different port:
```bash
plcbundle serve -port 9000
```

Or find what's using port 8080:
```bash
# macOS/Linux
lsof -i :8080

# Kill it if needed
kill <PID>
```

---

### WebSocket Connection Drops

**Problem:** WebSocket streaming stops after a few minutes.

**Causes:**
- Reverse proxy timeout
- Network timeout
- Client timeout

**Solutions:**

1. **Increase client timeout** (if using websocat):
```bash
websocat -t ws://localhost:8080/ws
```

2. **Configure reverse proxy** (nginx):
```nginx
location /ws {
    proxy_pass http://localhost:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 86400;  # 24 hours
}
```

---

### Memory Usage High

**Problem:** `plcbundle` using lots of RAM during rebuild.

**Cause:** Large bundles being processed simultaneously.

**Solution:**

Reduce workers:
```bash
plcbundle rebuild -workers 2
```

Or increase system limits:
```bash
# Check current limits
ulimit -a

# Increase if needed (Linux)
ulimit -v 4000000  # 4GB virtual memory
```

---

### Can't Clone from Remote

**Problem:**
```
Error loading remote index: failed to download: connection refused
```

**Checklist:**

1. **Is URL correct?**
```bash
curl https://remote-server.com/index.json
```

2. **Is server running?**
```bash
# On remote server
plcbundle serve
```

3. **Firewall blocking?**
```bash
# Test connectivity
telnet remote-server.com 8080
```

4. **HTTPS certificate issues?**
```bash
# Test with curl
curl -v https://remote-server.com/index.json
```

---

## Quick Reference

```bash
# Sync
plcbundle fetch              # Fetch next bundle
plcbundle fetch -count 0     # Fetch all available
plcbundle clone <url>        # Clone from remote

# Manage
plcbundle info               # Show repository info
plcbundle info -bundle 42    # Show specific bundle
plcbundle rebuild            # Rebuild index
plcbundle verify             # Verify chain

# Export
plcbundle export -count 1000           # Export operations
plcbundle backfill > all.jsonl         # Export everything
plcbundle mempool -export > mem.jsonl  # Export mempool

# Serve
plcbundle serve                     # Basic HTTP server
plcbundle serve -sync -websocket    # Full-featured server

# Utilities
plcbundle compare <url>        # Compare with remote
plcbundle mempool              # Check mempool status
plcbundle version              # Show version
```

---

## Getting Help

**Command help:**
```bash
plcbundle fetch -h
```