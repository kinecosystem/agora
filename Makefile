GO_OS := $(shell go env GOOS)
GO_ARCH := $(shell go env GOARCH)
GO_FLAGS :=

all: clean deps test build

.PHONY: clean
clean:
	@rm -rf service/transaction/build/
	@rm -f coverage.txt

.PHONY: deps
deps:
	@go get ./...

.PHONY: deps-clean
deps-clean:
	@go mod tidy

.PHONY: test
test:
	@./go-test.sh

.PHONY: build build-transaction
build: build-transaction
build-transaction:
	@go vet github.com/kinecosystem/meridian-transaction-services/service/transaction
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o service/transaction/build/$(GO_OS)-$(GO_ARCH)/transaction \
		github.com/kinecosystem/meridian-transaction-services/service/transaction
