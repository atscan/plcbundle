#!/bin/bash
# get-random-dids.sh - Extract N DIDs from random positions

N=${1:-10}

# Get bundle range
read FIRST LAST < <(plcbundle info 2>/dev/null | awk '/Range:/ {print $2, $4}' | tr -d '→')

echo "Sampling $N DIDs from bundles $FIRST-$LAST" >&2
echo "" >&2

for i in $(seq 1 $N); do
    BUNDLE=$(jot -r 1 $FIRST $LAST)
    POS=$(jot -r 1 0 9999)
    
    echo "[$i/$N] Bundle $BUNDLE, position $POS" >&2
    
    plcbundle get-op $BUNDLE $POS 2>/dev/null | jq -r '.did'
done | jq -R . | jq -s .

echo "" >&2
echo "✓ Done" >&2
