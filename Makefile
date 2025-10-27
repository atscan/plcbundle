.PHONY: all build install test clean fmt lint help

# Binary name
BINARY_NAME=plcbundle
INSTALL_PATH=$(GOPATH)/bin

# Go commands
GOCMD=go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOFMT=$(GOCMD) fmt
GOMOD=$(GOCMD) mod

# Default target
all: build

# Build the CLI tool
build:
	@echo "Building $(BINARY_NAME)..."
	$(GOBUILD) -o $(BINARY_NAME) ./cmd/plcbundle

# Install the CLI tool globally
install:
	@echo "Installing $(BINARY_NAME)..."
	$(GOINSTALL) ./cmd/plcbundle

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

# Show help
help:
	@echo "Available targets:"
	@echo "  make build          - Build the CLI tool"
	@echo "  make install        - Install CLI tool globally"
	@echo "  make test           - Run tests"
	@echo "  make test-coverage  - Run tests with coverage"
	@echo "  make clean          - Clean build artifacts"
	@echo "  make fmt            - Format code"
	@echo "  make deps           - Download dependencies"
	@echo "  make verify         - Verify dependencies"
	@echo "  make help           - Show this help"
