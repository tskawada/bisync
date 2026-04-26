BINARY     := bisync
BUILD_DIR  := ./bin
CMD        := ./cmd/bisync
PROTO_DIR  := ./proto
GEN_DIR    := ./internal/grpc/gen

VERSION    := 0.1.0-draft
LDFLAGS    := -ldflags "-X main.version=$(VERSION)"

.PHONY: all build build-arm64 build-armv7 build-all test lint clean generate install

all: build

## build: compile the daemon binary (host arch)
build:
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY) $(CMD)

## build-arm64: cross-compile for Linux arm64 (aarch64)
build-arm64:
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY)-arm64 $(CMD)

## build-armv7: cross-compile for Linux armv7 (32-bit ARM)
build-armv7:
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=arm GOARM=7 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY)-armv7 $(CMD)

## build-all: compile for amd64, arm64, armv7
build-all: build build-arm64 build-armv7

## test: run all unit tests
test:
	go test ./...

## test-race: run tests with race detector
test-race:
	go test -race ./...

## lint: run golangci-lint
lint:
	golangci-lint run ./...

## generate: regenerate gRPC code from proto (requires protoc + plugins)
generate:
	@which protoc > /dev/null 2>&1 || (echo "protoc not found. Install: https://grpc.io/docs/protoc-installation/" && exit 1)
	@which protoc-gen-go > /dev/null 2>&1 || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@which protoc-gen-go-grpc > /dev/null 2>&1 || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	mkdir -p $(GEN_DIR)
	protoc \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		-I$(PROTO_DIR) $(PROTO_DIR)/bisync.proto

## install: install binary to /usr/local/bin
install: build
	install -m 755 $(BUILD_DIR)/$(BINARY) /usr/local/bin/$(BINARY)

## tidy: update go.mod and go.sum
tidy:
	go mod tidy

## clean: remove build artifacts
clean:
	rm -rf $(BUILD_DIR)

## help: show this help
help:
	@grep -E '^##' Makefile | sed 's/## //'
