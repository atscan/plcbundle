#!/bin/bash
# test-did-resolution.sh
# Compares local DID resolution with plc.directory

set -e

BUNDLE_RANGE="${1:-1}"
PLC_URL="https://plc.wtf"
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "Testing DID resolution for bundle(s): $BUNDLE_RANGE"
echo "Temp directory: $TEMP_DIR"
echo ""

# Extract unique DIDs from bundle(s)
echo "Extracting DIDs from bundles..."
DIDS=$(plcbundle export --bundles "$BUNDLE_RANGE" | jq -r '.did' | sort -u)
DID_COUNT=$(echo "$DIDS" | wc -l | tr -d ' ')

echo "Found $DID_COUNT unique DIDs"
echo ""

# Test counters
TOTAL=0
PASSED=0
FAILED=0
ERRORS=0

# Test each DID
while IFS= read -r DID; do
    TOTAL=$((TOTAL + 1))
    
    # Progress
    echo -ne "\r[$TOTAL/$DID_COUNT] Testing $DID..."
    
    # Resolve locally
    LOCAL_FILE="$TEMP_DIR/local.json"
    if ! plcbundle index resolve "$DID" 2>/dev/null | jq --sort-keys . > "$LOCAL_FILE" 2>/dev/null; then
        echo -e "\r[$TOTAL/$DID_COUNT] ${RED}ERROR${NC} $DID (local resolution failed)"
        ERRORS=$((ERRORS + 1))
        continue
    fi
    
    # Resolve remotely
    REMOTE_FILE="$TEMP_DIR/remote.json"
    if ! curl -s "$PLC_URL/$DID" | jq --sort-keys . > "$REMOTE_FILE" 2>/dev/null; then
        echo -e "\r[$TOTAL/$DID_COUNT] ${YELLOW}SKIP${NC} $DID (remote fetch failed)"
        ERRORS=$((ERRORS + 1))
        continue
    fi
    
    # Compare
    if diff -q "$LOCAL_FILE" "$REMOTE_FILE" > /dev/null 2>&1; then
        echo -e "\r[$TOTAL/$DID_COUNT] ${GREEN}✓${NC} $DID"
        PASSED=$((PASSED + 1))
    else
        echo -e "\r[$TOTAL/$DID_COUNT] ${RED}✗${NC} $DID"
        echo ""
        echo "Differences found:"
        diff -u "$REMOTE_FILE" "$LOCAL_FILE" | head -20
        echo ""
        FAILED=$((FAILED + 1))
        
        # Ask to continue
        if [ $FAILED -ge 3 ]; then
            echo ""
            read -p "Continue testing? (y/N) " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                break
            fi
        fi
    fi
    
    # Rate limit
    sleep 0.1
    
done <<< "$DIDS"

# Summary
echo ""
echo "========================================="
echo "Test Results"
echo "========================================="
echo -e "Total:   $TOTAL"
echo -e "${GREEN}Passed:  $PASSED${NC}"
echo -e "${RED}Failed:  $FAILED${NC}"
echo -e "${YELLOW}Errors:  $ERRORS${NC}"
echo ""

if [ $FAILED -eq 0 ] && [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi
