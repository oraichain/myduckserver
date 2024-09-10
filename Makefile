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
.PHONY: test-full test cover

TEST_CMD = go test -cover -coverprofile cover.out 
SHOW_TOTAL_COVERAGE = go tool cover -func=cover.out | grep total | awk '{print "Total Coverage: " $$3 " (run '\''make cover'\'' for details)"}'

test-full:
	$(TEST_CMD) ./...
	@$(SHOW_TOTAL_COVERAGE)

test:
	$(TEST_CMD) $(shell go list ./... | grep -v './binlogreplication')
	@$(SHOW_TOTAL_COVERAGE)

cover:
	go tool cover -html=cover.out
