# CLI Guide

A concise guide to using the `plcbundle` command-line tool.

## Installation

```bash
go install tangled.org/atscan.net/plcbundle/cmd/plcbundle@latest
plcbundle version  # Verify installation
```

## Quick Start

```bash
mkdir plc_archive && cd plc_archive
plcbundle fetch -count 1        # Fetch one bundle
plcbundle info                  # Check what you have
```

---

## Commands

### `fetch` - Download from PLC Directory

Fetches operations from PLC directory and creates bundles.

```bash
plcbundle fetch -count 1        # Fetch exactly 1 bundle
plcbundle fetch -count 10       # Fetch 10 bundles
plcbundle fetch                 # Fetch continuously until caught up
```

**Important:** Without `-count`, fetch runs indefinitely. Always use `-count N` for controlled fetching.

**Options:**
- `-count N` - Number of bundles to fetch (0 = all available)
- `-plc URL` - Custom PLC directory URL (default: `https://plc.directory`)

---

### `clone` - Download from Remote Server

Downloads pre-made bundles from another plcbundle server (much faster than fetch).

```bash
plcbundle clone https://plc.example.com
plcbundle clone https://plc.example.com -workers 16    # Faster with more workers
```

**Resumable:** Press Ctrl+C to stop, run again to resume.

**Options:**
- `-workers N` - Concurrent downloads (default: 4)
- `-v` - Verbose output
- `-skip-existing` - Skip existing bundles (default: true)

---

### `info` - View Archive Status

Shows bundle count, storage size, time ranges, and chain hashes.

```bash
plcbundle info                  # General overview
plcbundle info -bundle 42       # Specific bundle details
plcbundle info --bundles        # List all bundles
plcbundle info --verify         # Info + chain verification
```

---

### `verify` - Check Integrity

Verifies file hashes and chain links.

```bash
plcbundle verify                # Verify entire chain
plcbundle verify -bundle 42     # Verify one bundle
plcbundle verify -v             # Verbose output
```

---

### `rebuild` - Recreate Index

Scans bundle files and rebuilds `index.json`.

```bash
plcbundle rebuild               # Auto-detect CPU cores
plcbundle rebuild -workers 8    # Use 8 workers
```

**When to use:**
- Lost/corrupted `index.json`
- Added bundle files manually
- Moved files from another location

---

### `export` - Extract Operations

Exports operations as JSONL to stdout.

```bash
plcbundle export -count 1000 > ops.jsonl
plcbundle export -after "2024-01-01T00:00:00Z" -count 5000 > jan.jsonl
```

---

### `backfill` - Stream All Operations

Streams operations from all bundles, fetching missing ones on-demand.

```bash
plcbundle backfill > all.jsonl
plcbundle backfill -start 100 -end 200 > range.jsonl
```

---

### `mempool` - Inspect Staging Area

Shows operations waiting to form a bundle (need 10,000 to create bundle).

```bash
plcbundle mempool               # Show status
plcbundle mempool -export > mem.jsonl    # Export mempool ops
plcbundle mempool -validate     # Verify chronological order
plcbundle mempool -clear        # Clear (destructive)
```

---

### `serve` - Run HTTP Server

Starts an HTTP server to share bundles with others.

```bash
plcbundle serve                           # Start on :8080
plcbundle serve -port 9000 -host 0.0.0.0  # Custom port/host
plcbundle serve -sync -sync-interval 5m   # Auto-fetch new bundles
plcbundle serve -websocket                # Enable WebSocket streaming
```

**Endpoints:**
- `GET /` - Info page
- `GET /index.json` - Bundle index
- `GET /data/:number` - Download bundle
- `WS /ws` - WebSocket stream (if enabled)

---

### `compare` - Compare with Remote

Shows differences between local and remote archives.

```bash
plcbundle compare https://plc.example.com
plcbundle compare https://plc.example.com --fetch-missing  # Auto-fix
```

---

### `version` - Show Version

```bash
plcbundle version
```

---

## Important Concepts

### Working Directory

plcbundle operates in your **current directory**. Always `cd` to your archive first:

```bash
cd /path/to/plc_archive
plcbundle info
```

### Files Created

```
plc_archive/
├── 000001.jsonl.zst           # Bundle files (10k ops each)
├── 000002.jsonl.zst
├── index.json                 # Index (metadata + hashes)
└── plc_mempool_*.jsonl        # Mempool (auto-managed, temporary)
```

### Fetch vs Clone

**Use `fetch`** when:
- No mirror available
- Want data directly from PLC
- Building from scratch

**Use `clone`** when:
- A mirror exists
- Want faster setup
- Syncing with known good source

---

## Common Tasks

**Initial setup from mirror:**
```bash
mkdir plc_archive && cd plc_archive
plcbundle clone https://plc.example.com -workers 16
plcbundle verify
```

**Initial setup from PLC:**
```bash
mkdir plc_archive && cd plc_archive
plcbundle fetch -count 0  # Fetch all (can take hours)
```

**Daily sync (cron):**
```bash
#!/bin/bash
cd /path/to/plc_archive
plcbundle fetch -count 5  # Fetch up to 5 new bundles
```

**Share your archive:**
```bash
plcbundle serve -host 0.0.0.0 -sync
```

**Export recent data:**
```bash
plcbundle export -count 10000 > recent.jsonl
cat recent.jsonl | jq .  # Process with jq
```

**Fix corrupted index:**
```bash
plcbundle rebuild
plcbundle verify
```

---

## Troubleshooting

**Command not found:**
```bash
export PATH=$PATH:$(go env GOPATH)/bin
```

**Wrong directory:**
```bash
pwd  # Check where you are
cd /path/to/plc_archive
```

**Fetch doesn't create bundle:**
```bash
plcbundle mempool  # Check if waiting for more ops
# Need 10,000 operations to create a bundle
```

**Port already in use:**
```bash
plcbundle serve -port 9000
```

**Hash verification failed:**
```bash
rm 000042.jsonl.zst       # Delete corrupted bundle
plcbundle rebuild         # Mark as missing
plcbundle fetch -count 1  # Re-fetch
```

**Out of disk space:**
```bash
df -h .  # Check space
# Move to larger disk or delete old bundles
```

---

## Quick Reference

```bash
# Fetch
plcbundle fetch -count 1              # One bundle
plcbundle fetch                       # All available

# Clone
plcbundle clone <url>                 # From mirror
plcbundle clone <url> -workers 16     # Faster

# Info
plcbundle info                        # Overview
plcbundle info -bundle 42             # Specific bundle

# Verify
plcbundle verify                      # Check chain
plcbundle verify -bundle 42           # Check one

# Rebuild
plcbundle rebuild                     # Recreate index

# Export
plcbundle export -count 1000 > ops.jsonl

# Serve
plcbundle serve                       # Share bundles
plcbundle serve -sync -websocket      # Full-featured

# Utilities
plcbundle mempool                     # Check staging
plcbundle compare <url>               # Compare with remote
plcbundle backfill > all.jsonl        # Export all
```

---

## Getting Help

```bash
plcbundle <command> -h    # Command-specific help
```

**Report issues:** https://tangled.org/@atscan.net/plcbundle/issues

