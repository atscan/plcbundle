#!/usr/bin/env bash
set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default configuration
ENDPOINT="${ENDPOINT:-http://localhost:8080}"
NUM_REQUESTS="${NUM_REQUESTS:-100}"
CONCURRENCY="${CONCURRENCY:-4}"
BUNDLE_DIR="${BUNDLE_DIR:-.}"
ROUTE="${ROUTE:-document}"
WARMUP="${WARMUP:-0}"
OUTPUT_CSV="${OUTPUT_CSV:-}"
VERBOSE="${VERBOSE:-0}"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--endpoint) ENDPOINT="$2"; shift 2 ;;
        -n|--requests) NUM_REQUESTS="$2"; shift 2 ;;
        -c|--concurrency) CONCURRENCY="$2"; shift 2 ;;
        -d|--dir) BUNDLE_DIR="$2"; shift 2 ;;
        -r|--route) ROUTE="$2"; shift 2 ;;
        -w|--warmup) WARMUP="$2"; shift 2 ;;
        -o|--output) OUTPUT_CSV="$2"; shift 2 ;;
        -v|--verbose) VERBOSE=1; shift ;;
        -h|--help)
            cat <<EOF
Usage: $0 [options]

Options:
  -e, --endpoint URL   HTTP endpoint (default: http://localhost:8080)
  -n, --requests N     Number of requests (default: 100)
  -c, --concurrency N  Concurrent requests (default: 4)
  -d, --dir PATH       Bundle directory (default: .)
  -r, --route TYPE     Route: document|data|audit (default: document)
  -w, --warmup N       Warmup requests (default: 0)
  -o, --output FILE    Save CSV results
  -v, --verbose        Verbose output

Examples:
  $0 -n 100 -c 4
  $0 -n 1000 -c 10 -w 50 -v -o results.csv
EOF
            exit 0
            ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
    esac
done

# Validate dependencies
for cmd in curl jq zstd; do
    if ! command -v $cmd &> /dev/null; then
        echo -e "${RED}Error: '$cmd' not found. Install with: brew install $cmd${NC}"
        exit 1
    fi
done

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}           DID Resolver HTTP Endpoint Benchmark${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Configuration:"
echo "  Endpoint:     $ENDPOINT"
echo "  Route:        /$ROUTE"
echo "  Requests:     $NUM_REQUESTS"
echo "  Concurrency:  $CONCURRENCY"
if [[ $WARMUP -gt 0 ]]; then
    echo "  Warmup:       $WARMUP"
fi
echo "  Bundle Dir:   $BUNDLE_DIR"
echo ""

# Setup temp
TEMP_DIR=$(mktemp -d)
trap "rm -rf '$TEMP_DIR'" EXIT

DID_LIST="$TEMP_DIR/dids.txt"
RESULTS_FILE="$TEMP_DIR/results.csv"

# Phase 1: Extract DIDs
echo -e "${CYAN}━━━ Phase 1: Extracting DIDs ━━━${NC}"
echo ""

# Find bundles
BUNDLES=()
while IFS= read -r line; do
    BUNDLES+=("$line")
done < <(find "$BUNDLE_DIR" -name "*.jsonl.zst" -type f | sort)

BUNDLE_COUNT=${#BUNDLES[@]}

if [[ $BUNDLE_COUNT -eq 0 ]]; then
    echo -e "${RED}Error: No bundles found in $BUNDLE_DIR${NC}"
    exit 1
fi

echo "  Found $BUNDLE_COUNT bundles"

# Sample bundles
SAMPLE_SIZE=10
if [[ $BUNDLE_COUNT -lt $SAMPLE_SIZE ]]; then
    SAMPLE_SIZE=$BUNDLE_COUNT
fi

echo "  Extracting DIDs from $SAMPLE_SIZE bundles..."
echo ""

# Extract DIDs properly (not in subshell)
> "$DID_LIST"  # Create empty file

for i in $(seq 0 $((SAMPLE_SIZE - 1))); do
    BUNDLE="${BUNDLES[$i]}"
    BUNDLE_NUM=$(basename "$BUNDLE" .jsonl.zst)
    
    echo -ne "\r  Bundle $BUNDLE_NUM ($((i + 1))/$SAMPLE_SIZE)...  "
    
    # Decompress and extract DIDs (show errors if verbose)
    if [[ $VERBOSE -eq 1 ]]; then
        zstd -dc "$BUNDLE" | jq -r '.did' >> "$DID_LIST"
    else
        zstd -dc "$BUNDLE" 2>/dev/null | jq -r '.did' 2>/dev/null >> "$DID_LIST" || {
            echo -e "\n${YELLOW}  Warning: Failed to process $BUNDLE_NUM${NC}"
            continue
        }
    fi
done

echo ""

# Deduplicate
sort -u "$DID_LIST" -o "$DID_LIST"

TOTAL_DIDS=$(wc -l < "$DID_LIST" | tr -d ' ')

if [[ $TOTAL_DIDS -eq 0 ]]; then
    echo -e "${RED}Error: No DIDs extracted. Check bundle files.${NC}"
    if [[ $VERBOSE -eq 0 ]]; then
        echo "Run with -v to see errors"
    fi
    exit 1
fi

echo -e "${GREEN}  ✓ Extracted $TOTAL_DIDS unique DIDs${NC}"
echo ""

# Adjust request count if needed
TOTAL_NEEDED=$((NUM_REQUESTS + WARMUP))
if [[ $TOTAL_DIDS -lt $TOTAL_NEEDED ]]; then
    echo -e "${YELLOW}  Warning: Only $TOTAL_DIDS DIDs available${NC}"
    if [[ $TOTAL_DIDS -le $WARMUP ]]; then
        WARMUP=0
        NUM_REQUESTS=$TOTAL_DIDS
    else
        NUM_REQUESTS=$((TOTAL_DIDS - WARMUP))
    fi
    TOTAL_NEEDED=$((NUM_REQUESTS + WARMUP))
    echo -e "${YELLOW}  Adjusted: $WARMUP warmup + $NUM_REQUESTS benchmark${NC}"
    echo ""
fi

# Select random DIDs
RANDOM_DIDS="$TEMP_DIR/random_dids.txt"
shuf -n $TOTAL_NEEDED "$DID_LIST" > "$RANDOM_DIDS"

# URL builder
get_url() {
    local did=$1
    case $ROUTE in
        data) echo "$ENDPOINT/$did/data" ;;
        audit) echo "$ENDPOINT/$did/log/audit" ;;
        *) echo "$ENDPOINT/$did" ;;
    esac
}

# Warmup
if [[ $WARMUP -gt 0 ]]; then
    echo -e "${CYAN}━━━ Phase 2: Warmup ━━━${NC}"
    echo ""
    echo "  Running $WARMUP warmup requests..."
    
    head -n $WARMUP "$RANDOM_DIDS" | while read did; do
        curl -s -o /dev/null "$(get_url "$did")" 2>/dev/null || true
    done
    
    echo -e "${GREEN}  ✓ Complete${NC}"
    echo ""
    
    tail -n +$((WARMUP + 1)) "$RANDOM_DIDS" > "$TEMP_DIR/bench.txt"
    RANDOM_DIDS="$TEMP_DIR/bench.txt"
fi

# Benchmark
PHASE_NUM=2
if [[ $WARMUP -gt 0 ]]; then
    PHASE_NUM=3
fi

echo -e "${CYAN}━━━ Phase $PHASE_NUM: Benchmark ━━━${NC}"
echo ""

# CSV header
echo "did,status,time_ms,size" > "$RESULTS_FILE"

# Benchmark function
benchmark_one() {
    local did=$1
    local url=$(get_url "$did")
    
    local start=$(date +%s%N)
    local output=$(curl -s -w "\n%{http_code}|%{size_download}" -o /dev/null "$url" 2>&1)
    local end=$(date +%s%N)
    
    local status=$(echo "$output" | tail -1 | cut -d'|' -f1)
    local size=$(echo "$output" | tail -1 | cut -d'|' -f2)
    local ms=$(( (end - start) / 1000000 ))
    
    # Print URL if 500 error
    if [[ "$status" == "500" ]]; then
        echo -e "${RED}✗ 500 Error - URL: $url${NC}" >&2
    fi
    
    echo "$did,$status,$ms,$size"
}

export -f benchmark_one
export -f get_url
export ENDPOINT ROUTE RED NC

# Execute
echo "  Running $NUM_REQUESTS requests (concurrency: $CONCURRENCY)..."

BENCH_START=$(date +%s%N)

if [[ $CONCURRENCY -eq 1 ]]; then
    count=0
    while IFS= read -r did; do
        benchmark_one "$did" >> "$RESULTS_FILE"
        count=$((count + 1))
        if (( count % 10 == 0 )); then
            printf "\r  Progress: %d/%d (%d%%)  " $count $NUM_REQUESTS $((count * 100 / NUM_REQUESTS))
        fi
    done < "$RANDOM_DIDS"
    echo ""
else
    cat "$RANDOM_DIDS" | xargs -P $CONCURRENCY -I {} bash -c 'benchmark_one "$@"' _ {} >> "$RESULTS_FILE" 2>&1
    echo -e "${GREEN}  ✓ Complete${NC}"
fi

BENCH_END=$(date +%s%N)
BENCH_SEC=$(awk "BEGIN {printf \"%.3f\", ($BENCH_END - $BENCH_START) / 1000000000}")

echo ""

# Results
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}                         Results${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Status codes
echo "HTTP Status Codes:"
echo "──────────────────"

tail -n +2 "$RESULTS_FILE" | cut -d',' -f2 | sort | uniq -c | sort -rn | while read count code; do
    case $code in
        200) echo -e "  ${GREEN}✓ 200${NC}: $count" ;;
        404) echo -e "  ${YELLOW}⚠ 404${NC}: $count" ;;
        410) echo -e "  ${YELLOW}⚠ 410${NC}: $count (deactivated)" ;;
        500) echo -e "  ${RED}✗ 500${NC}: $count" ;;
        *) echo "  ? $code: $count" ;;
    esac
done

echo ""

# Successful requests only
SUCCESS_FILE="$TEMP_DIR/success.csv"
tail -n +2 "$RESULTS_FILE" | awk -F',' '$2 == "200"' > "$SUCCESS_FILE" || true
SUCCESS_COUNT=$(wc -l < "$SUCCESS_FILE" | tr -d ' ')

if [[ $SUCCESS_COUNT -gt 0 ]]; then
    echo "Response Times:"
    echo "───────────────"
    
    # Extract and sort times
    TIMES="$TEMP_DIR/times.txt"
    cut -d',' -f3 "$SUCCESS_FILE" | sort -n > "$TIMES"
    
    # Stats
    MIN=$(head -1 "$TIMES")
    MAX=$(tail -1 "$TIMES")
    MEAN=$(awk '{sum+=$1} END {printf "%.1f", sum/NR}' "$TIMES")
    
    # Percentiles (safe calculation)
    calc_line() {
        local pct=$1
        local line=$(awk "BEGIN {print int($SUCCESS_COUNT * $pct)}")
        [[ $line -lt 1 ]] && line=1
        echo $line
    }
    
    P50=$(sed -n "$(calc_line 0.50)p" "$TIMES")
    P90=$(sed -n "$(calc_line 0.90)p" "$TIMES")
    P95=$(sed -n "$(calc_line 0.95)p" "$TIMES")
    P99=$(sed -n "$(calc_line 0.99)p" "$TIMES")
    
    echo "  Count:   $SUCCESS_COUNT"
    echo "  Min:     ${MIN} ms"
    echo "  Mean:    ${MEAN} ms"
    echo "  p50:     ${P50} ms"
    echo "  p90:     ${P90} ms"
    echo "  p95:     ${P95} ms"
    echo "  p99:     ${P99} ms"
    echo "  Max:     ${MAX} ms"
    echo ""
    
    echo "Throughput:"
    echo "───────────"
    RPS=$(awk "BEGIN {printf \"%.2f\", $SUCCESS_COUNT / $BENCH_SEC}")
    echo "  Duration:      ${BENCH_SEC}s"
    echo "  Requests/sec:  $RPS"
    echo ""
    
    # Distribution
    echo "Distribution:"
    echo "─────────────"
    
    ranges=(
        "0:5:<5ms"
        "5:10:5-10ms"
        "10:20:10-20ms"
        "20:50:20-50ms"
        "50:100:50-100ms"
        "100:500:100-500ms"
        "500:999999:>500ms"
    )
    
    for range in "${ranges[@]}"; do
        IFS=':' read -r min max label <<< "$range"
        count=$(awk -F',' -v min=$min -v max=$max '$3 >= min && $3 < max {c++} END {print c+0}' "$SUCCESS_FILE")
        
        if [[ $count -gt 0 ]]; then
            pct=$(awk "BEGIN {printf \"%.1f\", $count/$SUCCESS_COUNT*100}")
            bar_len=$(awk "BEGIN {printf \"%.0f\", $count/$SUCCESS_COUNT*30}")
            bar=$(printf '█%.0s' $(seq 1 $bar_len))
            printf "  %-10s [%-30s] %4d (%5.1f%%)\n" "$label" "$bar" $count $pct
        fi
    done
    
else
    echo -e "${RED}No successful requests!${NC}"
    echo ""
    echo "Debugging info:"
    echo "  Test connection: curl -v $ENDPOINT/status"
    echo "  Check results: cat $RESULTS_FILE"
fi

echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"

# Save output
if [[ -n "$OUTPUT_CSV" ]]; then
    cp "$RESULTS_FILE" "$OUTPUT_CSV"
    echo -e "${GREEN}✓ Saved to: $OUTPUT_CSV${NC}"
    echo ""
fi

# Verbose extras
if [[ $VERBOSE -eq 1 ]] && [[ $SUCCESS_COUNT -gt 0 ]]; then
    echo ""
    echo "Top 5 slowest:"
    tail -n +2 "$RESULTS_FILE" | awk -F',' '$2=="200"' | sort -t',' -k3 -rn | head -5 | \
        awk -F',' '{printf "  %5d ms - %s\n", $3, $1}'
    echo ""
    echo "Top 5 fastest:"
    tail -n +2 "$RESULTS_FILE" | awk -F',' '$2=="200"' | sort -t',' -k3 -n | head -5 | \
        awk -F',' '{printf "  %5d ms - %s\n", $3, $1}'
fi

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"

# Exit code
if [[ ${SUCCESS_COUNT:-0} -eq 0 ]]; then
    exit 1
fi
