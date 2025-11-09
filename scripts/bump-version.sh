#!/bin/bash

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

# Parse version (supports semver with pre-release)
# Format: MAJOR.MINOR.PATCH[-PRERELEASE]
if [[ $CURRENT_VERSION =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-([a-zA-Z]+)\.?([0-9]+)?)?$ ]]; then
    MAJOR="${BASH_REMATCH[1]}"
    MINOR="${BASH_REMATCH[2]}"
    PATCH="${BASH_REMATCH[3]}"
    PRERELEASE="${BASH_REMATCH[5]}"  # alpha, beta, rc, etc.
    PRERELEASE_NUM="${BASH_REMATCH[6]}"  # 1, 2, 3, etc.
else
    echo -e "${RED}Error: Could not parse version: ${CURRENT_VERSION}${NC}"
    exit 1
fi

# Function to show usage
show_usage() {
    echo "Usage: $0 {major|minor|patch|alpha|beta|rc|pre|release} [--minor|--patch]"
    echo ""
    echo "Stable releases:"
    echo "  major           Bump major version (x.0.0)"
    echo "  minor           Bump minor version (0.x.0)"
    echo "  patch           Bump patch version (0.0.x)"
    echo ""
    echo "Pre-releases:"
    echo "  alpha           Create/bump alpha (0.x.0-alpha.1 by default)"
    echo "  beta            Create/bump beta (0.x.0-beta.1 by default)"
    echo "  rc              Create/bump RC (0.x.0-rc.1 by default)"
    echo "  pre             Bump current pre-release number"
    echo ""
    echo "Combined (create pre-release with specific bump):"
    echo "  alpha --minor   Bump minor and create alpha (0.x.0-alpha.1)"
    echo "  alpha --patch   Bump patch and create alpha (0.0.x-alpha.1)"
    echo "  beta --minor    Bump minor and create beta"
    echo "  rc --patch      Bump patch and create rc"
    echo ""
    echo "Promotion:"
    echo "  release         Remove pre-release suffix (promote to stable)"
    echo ""
    echo "Examples:"
    echo "  v0.4.26 → minor          → v0.5.0"
    echo "  v0.4.26 → alpha --minor  → v0.5.0-alpha.1"
    echo "  v0.4.26 → alpha --patch  → v0.4.27-alpha.1"
    echo "  v0.5.0-alpha.1 → pre     → v0.5.0-alpha.2"
    echo "  v0.5.0-alpha.2 → beta    → v0.5.0-beta.1"
    echo "  v0.5.0-rc.1 → release    → v0.5.0"
    exit 1
}

# Parse optional flags (--minor, --patch)
BUMP_TYPE=""
if [[ "$2" == "--minor" ]]; then
    BUMP_TYPE="minor"
elif [[ "$2" == "--patch" ]]; then
    BUMP_TYPE="patch"
fi

# Determine new version based on command
case "$1" in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        PRERELEASE=""
        PRERELEASE_NUM=""
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        PRERELEASE=""
        PRERELEASE_NUM=""
        ;;
    patch)
        PATCH=$((PATCH + 1))
        PRERELEASE=""
        PRERELEASE_NUM=""
        ;;
    alpha|beta|rc)
        # If already on this pre-release type, bump the number
        if [[ "$PRERELEASE" == "$1" ]]; then
            PRERELEASE_NUM=$((PRERELEASE_NUM + 1))
        else
            # Creating new pre-release type
            if [[ -z "$PRERELEASE" ]]; then
                # Coming from stable - determine what to bump
                if [[ "$BUMP_TYPE" == "minor" ]]; then
                    MINOR=$((MINOR + 1))
                    PATCH=0
                elif [[ "$BUMP_TYPE" == "patch" ]]; then
                    PATCH=$((PATCH + 1))
                else
                    # Default: bump minor for new pre-release
                    MINOR=$((MINOR + 1))
                    PATCH=0
                fi
            fi
            # Set new pre-release
            PRERELEASE="$1"
            PRERELEASE_NUM=1
        fi
        ;;
    pre|prerelease)
        if [[ -z "$PRERELEASE" ]]; then
            echo -e "${RED}Error: Not currently on a pre-release version${NC}"
            echo "Current version: ${CURRENT_VERSION}"
            echo "Use 'alpha', 'beta', or 'rc' to create a pre-release"
            exit 1
        fi
        PRERELEASE_NUM=$((PRERELEASE_NUM + 1))
        ;;
    release|stable)
        if [[ -z "$PRERELEASE" ]]; then
            echo -e "${YELLOW}Already on stable version: ${CURRENT_VERSION}${NC}"
            exit 0
        fi
        # Remove pre-release suffix
        PRERELEASE=""
        PRERELEASE_NUM=""
        ;;
    -h|--help|help)
        show_usage
        ;;
    "")
        echo -e "${RED}Error: No command specified${NC}"
        echo ""
        show_usage
        ;;
    *)
        echo -e "${RED}Error: Invalid command: $1${NC}"
        echo ""
        show_usage
        ;;
esac

# Build new version string
if [[ -z "$PRERELEASE" ]]; then
    NEW_VERSION="v${MAJOR}.${MINOR}.${PATCH}"
else
    NEW_VERSION="v${MAJOR}.${MINOR}.${PATCH}-${PRERELEASE}.${PRERELEASE_NUM}"
fi

# Show version change
echo ""
echo -e "${YELLOW}Current version: ${CURRENT_VERSION}${NC}"
echo -e "${GREEN}New version:     ${NEW_VERSION#v}${NC}"

# Show what changed
if [[ -n "$PRERELEASE" ]]; then
    echo -e "${BLUE}Pre-release:     ${PRERELEASE}.${PRERELEASE_NUM}${NC}"
    echo -e "${YELLOW}⚠️  This is a pre-release (no stability guarantees)${NC}"
else
    echo -e "${GREEN}✓ Stable release${NC}"
fi

echo ""

# Confirm
read -p "Create tag ${NEW_VERSION}? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled"
    exit 0
fi

# Create git tag
if [[ -n "$PRERELEASE" ]]; then
    # Pre-release tag
    git tag -a "${NEW_VERSION}" -m "Pre-release ${NEW_VERSION}"
    echo -e "${GREEN}✓ Created pre-release tag ${NEW_VERSION}${NC}"
else
    # Stable release tag
    git tag -a "${NEW_VERSION}" -m "Release ${NEW_VERSION}"
    echo -e "${GREEN}✓ Created release tag ${NEW_VERSION}${NC}"
fi

echo ""
echo "To push the tag, run:"
echo "  git push origin ${NEW_VERSION}"
echo ""
echo "Or use 'make release' to push automatically"
