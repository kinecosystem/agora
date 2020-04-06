GO_OS := $(shell go env GOOS)
GO_ARCH := $(shell go env GOARCH)
GO_FLAGS :=

ENV := dev
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

all: clean deps test images

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
	@go vet github.com/kinecosystem/agora-transaction-services-internal/service/transaction
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o service/transaction/build/$(GO_OS)-$(GO_ARCH)/transaction \
		github.com/kinecosystem/agora-transaction-services-internal/service/transaction

.PHONY: images
images: GO_OS := linux
images: GO_ARCH := amd64
images: build images-only

.PHONY: images-only transaction-image
images-only: transaction-image
transaction-image:
	docker build service/transaction -t transaction-service:$(GIT_BRANCH)

.PHONY: deploy-transaction
deploy-transaction: GO_OS=linux
deploy-transaction: build
deploy-transaction: transaction-image
deploy-transaction:
	cddc deploy --service-config service/transaction/service.yaml -p agora$(ENV) -t $(GIT_BRANCH)
