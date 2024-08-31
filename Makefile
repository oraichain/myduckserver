# Project name (name of the executable)
BINARY_NAME := myduckserver

# Go source directory
SRC_DIR := .

# Go source files
SRC_FILES := $(wildcard $(SRC_DIR)/*.go)

# Version information
VERSION := $(shell git describe --tags --always --dirty)
BUILD_TIME := $(shell date +%Y-%m-%dT%H:%M:%S%z)
GIT_COMMIT := $(shell git rev-parse HEAD)

# Compilation flags
LDFLAGS := -X 'main.Version=$(VERSION)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)'

# Build target
$(BINARY_NAME): $(SRC_FILES)
	go build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME) $(SRC_DIR)

# Default target
.PHONY: all build
all: build
build: $(BINARY_NAME)

# Clean target
.PHONY: clean
clean:
	rm -f $(BINARY_NAME)

# Run target
.PHONY: run
run: build
	./$(BINARY_NAME)

# Test target
.PHONY: test
test:
	go test -cover ./...
