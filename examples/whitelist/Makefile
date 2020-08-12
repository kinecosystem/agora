GO_OS := $(shell go env GOOS)
GO_ARCH := $(shell go env GOARCH)

.PHONY: build
build:
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o build/$(GO_OS)-$(GO_ARCH)/main \
		github.com/kinecosystem/agora/examples/whitelist

.PHONY: image
image: GO_OS := linux
image: GO_ARCH := amd64
image:
	docker build . -t agora-whitelister
