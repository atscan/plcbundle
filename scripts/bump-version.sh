#!/bin/bash

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if git is clean
if [[ -n $(git status -s) ]]; then
    echo -e "${RED}Error: Git working directory is not clean${NC}"
    echo "Please commit or stash your changes first"
    exit 1
fi

# Get current version from git tags
CURRENT_VERSION=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")

# Remove 'v' prefix if present
CURRENT_VERSION=${CURRENT_VERSION#v}

# Parse version
IFS='.' read -r -a VERSION_PARTS <<< "$CURRENT_VERSION"
MAJOR="${VERSION_PARTS[0]}"
MINOR="${VERSION_PARTS[1]}"
PATCH="${VERSION_PARTS[2]}"

# Bump version based on argument
case "$1" in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        ;;
    patch)
        PATCH=$((PATCH + 1))
        ;;
    *)
        echo "Usage: $0 {major|minor|patch}"
        exit 1
        ;;
esac

NEW_VERSION="v${MAJOR}.${MINOR}.${PATCH}"

echo -e "${YELLOW}Current version: ${CURRENT_VERSION}${NC}"
echo -e "${GREEN}New version:     ${NEW_VERSION}${NC}"
echo ""

# Confirm
read -p "Create tag ${NEW_VERSION}? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled"
    exit 0
fi

# Create git tag
git tag -a "${NEW_VERSION}" -m "Release ${NEW_VERSION}"

echo -e "${GREEN}✓ Created tag ${NEW_VERSION}${NC}"
echo ""
echo "To push the tag, run:"
echo "  git push origin ${NEW_VERSION}"
echo ""
echo "Or use 'make release' to push automatically"
