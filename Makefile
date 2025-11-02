.PHONY: all build install test clean fmt lint help version bump-patch bump-minor bump-major release release-build
.PHONY: docker-build docker-buildx docker-push docker-run docker-clean docker-shell compose-up compose-down compose-logs

# Binary name
BINARY_NAME=plcbundle
INSTALL_PATH=$(GOPATH)/bin

# Docker configuration
DOCKER_IMAGE=plcbundle
DOCKER_TAG=$(VERSION)
DOCKER_REGISTRY?=atscan
DOCKER_FULL_IMAGE=$(if $(DOCKER_REGISTRY),$(DOCKER_REGISTRY)/,)$(DOCKER_IMAGE):$(DOCKER_TAG)
DOCKER_LATEST=$(if $(DOCKER_REGISTRY),$(DOCKER_REGISTRY)/,)$(DOCKER_IMAGE):latest
DOCKER_PLATFORMS?=linux/amd64,linux/arm64

# Version information
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u '+%Y-%m-%d_%H:%M:%S')

# Go commands
GOCMD=go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOFMT=$(GOCMD) fmt
GOMOD=$(GOCMD) mod

# Build flags
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(GIT_COMMIT) -X main.buildDate=$(BUILD_DATE)"

# Default target
all: build

# Build the CLI tool with version info
build:
	@echo "Building $(BINARY_NAME) $(VERSION)..."
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) ./cmd/plcbundle

# Install the CLI tool globally
install:
	@echo "Installing $(BINARY_NAME) $(VERSION)..."
	$(GOINSTALL) $(LDFLAGS) ./cmd/plcbundle

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -cover ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -rf dist/

# Format code
fmt:
	@echo "Formatting code..."
	$(GOFMT) ./...

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# Verify dependencies
verify:
	@echo "Verifying dependencies..."
	$(GOMOD) verify

# Show current version
version:
	@echo "Current version: $(VERSION)"
	@echo "Git commit: $(GIT_COMMIT)"
	@echo "Build date: $(BUILD_DATE)"

# Bump patch version (0.1.0 -> 0.1.1)
bump-patch:
	@echo "Bumping patch version..."
	@./scripts/bump-version.sh patch

# Bump minor version (0.1.0 -> 0.2.0)
bump-minor:
	@echo "Bumping minor version..."
	@./scripts/bump-version.sh minor

# Bump major version (0.1.0 -> 1.0.0)
bump-major:
	@echo "Bumping major version..."
	@./scripts/bump-version.sh major

# Create release
release:
	@echo "Creating release for version $(VERSION)..."
	@./scripts/release.sh

# ============================================================================
# Docker Commands
# ============================================================================

# Build Docker image (local)
docker-build:
	@echo "Building Docker image $(DOCKER_FULL_IMAGE)..."
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") \
		-t $(DOCKER_FULL_IMAGE) \
		-t $(DOCKER_LATEST) \
		.
	@echo "✓ Built: $(DOCKER_FULL_IMAGE)"

# Build multi-platform and push
docker-buildx:
	@echo "Building multi-platform image $(DOCKER_FULL_IMAGE)..."
	docker buildx build \
		--platform $(DOCKER_PLATFORMS) \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
  		--build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") \		
		--tag $(DOCKER_FULL_IMAGE) \
		--tag $(DOCKER_LATEST) \
		--push \
		.
	@echo "✓ Built and pushed: $(DOCKER_FULL_IMAGE) ($(DOCKER_PLATFORMS))"

# Push Docker image
docker-push:
	@echo "Pushing Docker image..."
	docker push $(DOCKER_FULL_IMAGE)
	docker push $(DOCKER_LATEST)
	@echo "✓ Pushed: $(DOCKER_FULL_IMAGE)"

# Run Docker container as CLI
docker-run:
	@docker run --rm -v $(PWD)/data:/data $(DOCKER_FULL_IMAGE) plcbundle $(CMD)

# Shortcuts
docker-info:
	@docker run --rm -v $(PWD)/data:/data $(DOCKER_FULL_IMAGE) plcbundle info

docker-fetch:
	@docker run --rm -v $(PWD)/data:/data $(DOCKER_FULL_IMAGE) plcbundle fetch

docker-verify:
	@docker run --rm -v $(PWD)/data:/data $(DOCKER_FULL_IMAGE) plcbundle verify

# Run as server
docker-serve:
	docker run --rm -it -p 8080:8080 -v $(PWD)/data:/data $(DOCKER_FULL_IMAGE) plcbundle serve --host 0.0.0.0

# Open shell
docker-shell:
	docker run --rm -it -v $(PWD)/data:/data $(DOCKER_FULL_IMAGE) sh

# Clean Docker artifacts
docker-clean:
	@echo "Cleaning Docker images..."
	docker rmi $(DOCKER_FULL_IMAGE) $(DOCKER_LATEST) 2>/dev/null || true
	docker image prune -f

# ============================================================================
# Help
# ============================================================================

help:
	@echo "Available targets:"
	@echo ""
	@echo "Build & Install:"
	@echo "  make build          - Build the CLI tool"
	@echo "  make install        - Install CLI tool globally"
	@echo "  make clean          - Clean build artifacts"
	@echo ""
	@echo "Development:"
	@echo "  make test           - Run tests"
	@echo "  make test-coverage  - Run tests with coverage"
	@echo "  make fmt            - Format code"
	@echo "  make deps           - Download dependencies"
	@echo ""
	@echo "Release:"
	@echo "  make version        - Show current version"
	@echo "  make bump-patch     - Bump patch (0.1.0 -> 0.1.1)"
	@echo "  make bump-minor     - Bump minor (0.1.0 -> 0.2.0)"
	@echo "  make bump-major     - Bump major (0.1.0 -> 1.0.0)"
	@echo "  make release        - Push tag to trigger release"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build   - Build image (current platform)"
	@echo "  make docker-buildx  - Build multi-platform and push"
	@echo "  make docker-run     - Run CLI (CMD='info')"
	@echo "  make docker-info    - Show bundle info"
	@echo "  make docker-serve   - Run as server"
	@echo "  make docker-shell   - Open shell"
	@echo ""
