#!/bin/bash

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Get latest tag
LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")

if [[ -z "$LATEST_TAG" ]]; then
    echo -e "${RED}Error: No tags found${NC}"
    echo "Create a tag first using: make bump-patch|bump-minor|bump-major"
    exit 1
fi

echo -e "${GREEN}Releasing version: ${LATEST_TAG}${NC}"
echo ""

# Check if tag is already pushed
if git ls-remote --tags origin | grep -q "${LATEST_TAG}"; then
    echo -e "${YELLOW}Warning: Tag ${LATEST_TAG} already exists on remote${NC}"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 0
    fi
fi

# Push tag
echo "Pushing tag ${LATEST_TAG}..."
git push origin "${LATEST_TAG}"

# Also push main/master branch
BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Pushing branch ${BRANCH}..."
git push origin "${BRANCH}"

echo ""
echo -e "${GREEN}✓ Release ${LATEST_TAG} published${NC}"
echo ""
echo "View releases at:"
echo "  https://tangled.org/@atscan.net/plcbundle/tags"
