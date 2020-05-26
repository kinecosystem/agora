GO_OS := $(shell go env GOOS)
GO_ARCH := $(shell go env GOARCH)
GO_FLAGS :=

ENV := dev
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

all: clean deps test images

.PHONY: clean
clean:
	@rm -rf service/agora/build/
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

.PHONY: build build-agora
build: build-agora
build-agora:
	@go vet github.com/kinecosystem/agora/service/agora
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o service/agora/build/$(GO_OS)-$(GO_ARCH)/agora \
		github.com/kinecosystem/agora/service/agora

.PHONY: images
images: GO_OS := linux
images: GO_ARCH := amd64
images: build images-only

.PHONY: images-only agora-image
images-only: agora-image
agora-image:
	docker build service/agora -t agora-service:$(GIT_BRANCH)

.PHONY: deploy-agora
deploy-agora: GO_OS=linux
deploy-agora: build
deploy-agora: agora-image
deploy-agora:
	cddc deploy --service-config service/agora/service.yaml -p agora$(ENV) -t $(GIT_BRANCH)
