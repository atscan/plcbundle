#!/bin/bash
# did-resolve-benchmark-http.sh - Benchmark DID resolution over HTTP

set -e

SERVER_URL=${1:-"http://localhost:8080"}
BUNDLE=${2:-1}
SAMPLES=${3:-20}

echo "═══════════════════════════════════════════════════════════"
echo "  DID Resolution HTTP Performance Benchmark"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "Server: $SERVER_URL"
echo "Bundle: $BUNDLE"
echo "Samples per position range: $SAMPLES"
echo ""

# Check if server is responding
if ! curl -s -f "$SERVER_URL/status" > /dev/null 2>&1; then
    echo "❌ Error: Server not responding at $SERVER_URL"
    echo ""
    echo "Start server first:"
    echo "  plcbundle serve --port 8080"
    exit 1
fi

echo "✓ Server is online"
echo ""

# Extract DIDs at different positions from local bundle
echo "Extracting test DIDs from bundle $BUNDLE..."

# Early positions (0-100)
EARLY_DIDS=$(plcbundle export --bundles $BUNDLE | head -100 | jq -r '.did' | head -$SAMPLES)

# Middle positions (~5000)
MIDDLE_DIDS=$(plcbundle export --bundles $BUNDLE | head -5100 | tail -100 | jq -r '.did' | head -$SAMPLES)

# Late positions (~9900)
LATE_DIDS=$(plcbundle export --bundles $BUNDLE | tail -100 | jq -r '.did' | head -$SAMPLES)

echo "✓ Extracted test DIDs"
echo ""

# Function to benchmark HTTP resolution
benchmark_http() {
    local label="$1"
    local dids="$2"
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Testing: $label"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    local total_time=0
    local total_dns=0
    local total_connect=0
    local total_starttransfer=0
    local count=0
    local min_time=999999
    local max_time=0
    local errors=0
    
    # Arrays to store individual timings
    local -a total_timings
    local -a transfer_timings
    
    for did in $dids; do
        # Use curl with timing
        timing=$(curl -s -w "\n%{time_namelookup},%{time_connect},%{time_starttransfer},%{time_total},%{http_code}" \
            -o /dev/null \
            "$SERVER_URL/$did" 2>&1)
        
        # Parse timing (last line)
        IFS=',' read -r dns connect starttransfer total http_code <<< "$(echo "$timing" | tail -1)"
        
        if [ "$http_code" == "200" ]; then
            # Convert to milliseconds
            total_ms=$(echo "scale=2; $total * 1000" | bc)
            transfer_ms=$(echo "scale=2; $starttransfer * 1000" | bc)
            dns_ms=$(echo "scale=2; $dns * 1000" | bc)
            connect_ms=$(echo "scale=2; $connect * 1000" | bc)
            
            total_timings+=($total_ms)
            transfer_timings+=($transfer_ms)
            
            total_time=$(echo "$total_time + $total_ms" | bc)
            total_dns=$(echo "$total_dns + $dns_ms" | bc)
            total_connect=$(echo "$total_connect + $connect_ms" | bc)
            total_starttransfer=$(echo "$total_starttransfer + $transfer_ms" | bc)
            count=$((count + 1))
            
            # Update min/max (total time)
            if (( $(echo "$total_ms < $min_time" | bc -l) )); then
                min_time=$total_ms
            fi
            if (( $(echo "$total_ms > $max_time" | bc -l) )); then
                max_time=$total_ms
            fi
            
            printf "."
        else
            printf "E"
            errors=$((errors + 1))
        fi
    done
    
    echo ""
    
    if [ $count -gt 0 ]; then
        avg_total=$(echo "scale=2; $total_time / $count" | bc)
        avg_dns=$(echo "scale=2; $total_dns / $count" | bc)
        avg_connect=$(echo "scale=2; $total_connect / $count" | bc)
        avg_transfer=$(echo "scale=2; $total_starttransfer / $count" | bc)
        
        # Calculate median total time
        IFS=$'\n' sorted=($(sort -n <<<"${total_timings[*]}"))
        median_idx=$((count / 2))
        median_total=${sorted[$median_idx]}
        
        # Calculate median transfer time
        IFS=$'\n' sorted_transfer=($(sort -n <<<"${transfer_timings[*]}"))
        median_transfer=${sorted_transfer[$median_idx]}
        
        echo ""
        echo "Results ($count successful, $errors errors):"
        echo ""
        echo "  Total Response Time:"
        echo "    Average:  ${avg_total}ms"
        echo "    Median:   ${median_total}ms"
        echo "    Min:      ${min_time}ms"
        echo "    Max:      ${max_time}ms"
        echo ""
        echo "  Breakdown (average):"
        echo "    DNS lookup:       ${avg_dns}ms"
        echo "    TCP connect:      ${avg_connect}ms"
        echo "    Time to first byte: ${avg_transfer}ms"
        echo "    Transfer:         $(echo "scale=2; $avg_total - $avg_transfer" | bc)ms"
    else
        echo "  ❌ All requests failed"
    fi
    
    echo ""
}

# Run HTTP benchmarks
benchmark_http "Early Positions (0-100)" "$EARLY_DIDS"
benchmark_http "Middle Positions (~5000)" "$MIDDLE_DIDS"
benchmark_http "Late Positions (~9900)" "$LATE_DIDS"

echo "═══════════════════════════════════════════════════════════"
echo "  HTTP Benchmark Complete"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "Note: HTTP adds overhead (~2-5ms) compared to local resolution"
echo "The 'Time to first byte' shows server processing time"
echo ""

# Bonus: Test server status endpoint
echo "Server Status:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
curl -s "$SERVER_URL/status" | jq -r '
  "  Bundles: \(.bundles.count)",
  "  Operations: \(.bundles.total_operations // 0)",
  "  Uptime: \(.server.uptime_seconds)s",
  "  WebSocket: \(.server.websocket_enabled)",
  if .mempool then "  Mempool: \(.mempool.count) ops" else "  Mempool: disabled" end
'
echo ""
