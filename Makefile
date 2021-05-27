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
	@rm -rf service/garbage-collector/build/
	@rm -rf service/history-collector/build/
	@rm -rf service/migrator/build/
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

.PHONY: build build-agora build-garbage-collector build-history-collector build-migrator
build: build-agora build-garbage-collector build-history-collector build-migrator
build-agora:
	@go vet github.com/kinecosystem/agora/service/agora
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o service/agora/build/$(GO_OS)-$(GO_ARCH)/agora \
		github.com/kinecosystem/agora/service/agora
build-garbage-collector:
	@go vet github.com/kinecosystem/agora/service/garbage-collector
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o service/garbage-collector/build/$(GO_OS)-$(GO_ARCH)/garbage-collector \
		github.com/kinecosystem/agora/service/garbage-collector
build-history-collector:
	@go vet github.com/kinecosystem/agora/service/history-collector
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o service/history-collector/build/$(GO_OS)-$(GO_ARCH)/history-collector \
		github.com/kinecosystem/agora/service/history-collector
build-migrator:
	@go vet github.com/kinecosystem/agora/service/migrator
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o service/migrator/build/$(GO_OS)-$(GO_ARCH)/migrator \
		github.com/kinecosystem/agora/service/migrator

.PHONY: images
images: GO_OS := linux
images: GO_ARCH := amd64
images: build images-only

.PHONY: images-only agora-image garbage-collector-image history-collector-image migrator-image
images-only: agora-image garbage-collector-image history-collector-image migrator-image
agora-image:
	docker build service/agora -t agora-service:$(GIT_BRANCH)
garbage-collector-image:
	docker build service/garbage-collector -t garbage-collector:$(GIT_BRANCH)
history-collector-image:
	docker build service/history-collector -t history-collector:$(GIT_BRANCH)
migrator-image:
	docker build service/migrator -t migrator-service:$(GIT_BRANCH)
