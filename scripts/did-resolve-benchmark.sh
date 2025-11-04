#!/bin/bash
# did-resolve-benchmar.sh - Benchmark DID resolution performance

set -e

BUNDLE=${1:-1}
SAMPLES=${2:-20}

echo "═══════════════════════════════════════════════════════════"
echo "  DID Resolution Performance Benchmark"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "Bundle: $BUNDLE"
echo "Samples per position range: $SAMPLES"
echo ""

# Extract DIDs at different positions from a bundle
echo "Extracting test DIDs from bundle $BUNDLE..."

# Early positions (0-100)
EARLY_DIDS=$(plcbundle export --bundles $BUNDLE | head -100 | jq -r '.did' | head -$SAMPLES)

# Middle positions (~5000)
MIDDLE_DIDS=$(plcbundle export --bundles $BUNDLE | head -5100 | tail -100 | jq -r '.did' | head -$SAMPLES)

# Late positions (~9900)
LATE_DIDS=$(plcbundle export --bundles $BUNDLE | tail -100 | jq -r '.did' | head -$SAMPLES)

echo "✓ Extracted test DIDs"
echo ""

# Function to benchmark a set of DIDs
benchmark_dids() {
    local label="$1"
    local dids="$2"
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Testing: $label"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    local total_time=0
    local count=0
    local min_time=999999
    local max_time=0
    
    # Arrays to store individual timings
    local -a timings
    
    for did in $dids; do
        # Run resolve and extract timing
        output=$(plcbundle index resolve "$did" 2>&1)
        
        # Extract "Total: XXms" from stderr
        if [[ $output =~ Total:\ ([0-9.]+)([µm]?s) ]]; then
            time_value="${BASH_REMATCH[1]}"
            time_unit="${BASH_REMATCH[2]}"
            
            # Convert to milliseconds
            if [[ $time_unit == "µs" ]]; then
                time_ms=$(echo "scale=3; $time_value / 1000" | bc)
            elif [[ $time_unit == "ms" ]]; then
                time_ms=$time_value
            else
                # Assume seconds
                time_ms=$(echo "scale=3; $time_value * 1000" | bc)
            fi
            
            timings+=($time_ms)
            total_time=$(echo "$total_time + $time_ms" | bc)
            count=$((count + 1))
            
            # Update min/max
            if (( $(echo "$time_ms < $min_time" | bc -l) )); then
                min_time=$time_ms
            fi
            if (( $(echo "$time_ms > $max_time" | bc -l) )); then
                max_time=$time_ms
            fi
            
            printf "."
        fi
    done
    
    echo ""
    
    if [ $count -gt 0 ]; then
        avg_time=$(echo "scale=2; $total_time / $count" | bc)
        
        # Calculate median (sort and take middle)
        IFS=$'\n' sorted=($(sort -n <<<"${timings[*]}"))
        median_idx=$((count / 2))
        median_time=${sorted[$median_idx]}
        
        echo ""
        echo "Results ($count samples):"
        echo "  Average:  ${avg_time}ms"
        echo "  Median:   ${median_time}ms"
        echo "  Min:      ${min_time}ms"
        echo "  Max:      ${max_time}ms"
    else
        echo "  No successful timings"
    fi
    
    echo ""
}

# Run benchmarks
benchmark_dids "Early Positions (0-100)" "$EARLY_DIDS"
benchmark_dids "Middle Positions (~5000)" "$MIDDLE_DIDS"
benchmark_dids "Late Positions (~9900)" "$LATE_DIDS"

echo "═══════════════════════════════════════════════════════════"
echo "  Benchmark Complete"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "Expected results with LoadOperation optimization:"
echo "  Early:  ~2-5ms   (only decompress first 1%)"
echo "  Middle: ~10-15ms (decompress ~50%)"
echo "  Late:   ~20-30ms (decompress ~99%)"
echo ""
echo "If all positions show similar timing (~18ms), the optimization"
echo "isn't working and LoadBundle is still being called."
echo ""
