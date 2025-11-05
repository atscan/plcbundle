#!/usr/bin/env bash

ENDPOINT="${1:-http://localhost:8080}"
NUM="${2:-100}"
DIR="${3:-.}"

echo "=== DID Resolver Benchmark ==="
echo "Endpoint: $ENDPOINT"
echo "Requests: $NUM"
echo ""

# Extract DIDs
echo "Extracting DIDs..."
TEMP=$(mktemp)
trap "rm -f $TEMP ${TEMP}.*" EXIT

find "$DIR" -name "*.jsonl.zst" | head -5 | while read f; do
    zstd -dc "$f" | jq -r '.did'
done | sort -u | shuf -n $NUM > "$TEMP"

TOTAL=$(wc -l < "$TEMP" | tr -d ' ')
echo "Testing $TOTAL DIDs"
echo ""

# Run benchmark
echo "Benchmarking..."
SUCCESS=0
FAILED=0

while read did; do
    START=$(date +%s%N)
    CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/$did" 2>/dev/null)
    END=$(date +%s%N)
    
    MS=$(( (END - START) / 1000000 ))
    
    if [[ $CODE == "200" ]]; then
        SUCCESS=$((SUCCESS + 1))
        echo "$MS" >> "${TEMP}.times"
    else
        FAILED=$((FAILED + 1))
    fi
    
    DONE=$((SUCCESS + FAILED))
    if (( DONE % 10 == 0 )); then
        printf "\r  %d/%d  " $DONE $TOTAL
    fi
done < "$TEMP"

echo ""
echo ""
echo "Results:"
echo "  Success: $SUCCESS"
echo "  Failed:  $FAILED"

if [[ $SUCCESS -gt 0 ]]; then
    sort -n "${TEMP}.times" -o "${TEMP}.times"
    
    MIN=$(head -1 "${TEMP}.times")
    MAX=$(tail -1 "${TEMP}.times")
    AVG=$(awk '{s+=$1} END {printf "%.0f", s/NR}' "${TEMP}.times")
    
    # Percentiles
    P50=$(sed -n "$(awk "BEGIN {print int($SUCCESS * 0.50)}")p" "${TEMP}.times")
    P90=$(sed -n "$(awk "BEGIN {print int($SUCCESS * 0.90)}")p" "${TEMP}.times")
    P95=$(sed -n "$(awk "BEGIN {print int($SUCCESS * 0.95)}")p" "${TEMP}.times")
    
    echo ""
    echo "  Min:  ${MIN}ms"
    echo "  Avg:  ${AVG}ms"
    echo "  p50:  ${P50}ms"
    echo "  p90:  ${P90}ms"
    echo "  p95:  ${P95}ms"
    echo "  Max:  ${MAX}ms"
fi