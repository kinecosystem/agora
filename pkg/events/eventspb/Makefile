all: generate

.PHONY: generate
generate:
	docker run -v $(shell pwd):/proto -v $(shell pwd):/genproto mfycheng/protoc-gen-go
