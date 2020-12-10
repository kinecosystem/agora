GO_OS := $(shell go env GOOS)
GO_ARCH := $(shell go env GOARCH)
GO_FLAGS :=

ENV := dev
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

.NOTPARALLEL:

all: clean deps test images

.PHONY: clean
clean:
	@rm -rf service/agora/build/
	@rm -rf service/history-collector/build/
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

.PHONY: build build-agora build-history-collector
build: build-agora build-history-collector
build-agora:
	@go vet github.com/kinecosystem/agora/service/agora
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o service/agora/build/$(GO_OS)-$(GO_ARCH)/agora \
		github.com/kinecosystem/agora/service/agora
build-history-collector:
	@go vet github.com/kinecosystem/agora/service/history-collector
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o service/history-collector/build/$(GO_OS)-$(GO_ARCH)/history-collector \
		github.com/kinecosystem/agora/service/history-collector

.PHONY: images
images: GO_OS := linux
images: GO_ARCH := amd64
images: build images-only

.PHONY: images-only agora-image history-collector-image
images-only: agora-image history-collector-image
agora-image:
	docker build service/agora -t agora-service:$(GIT_BRANCH)
history-collector-image:
	docker build service/history-collector -t history-collector:$(GIT_BRANCH)
