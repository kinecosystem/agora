USER_ID := $(shell id -u)
GROUP_ID := $(shell id -g)

all: generate

.PHONY: generate
generate:
	docker run -v $(shell pwd):/proto -v $(shell pwd):/genproto --user $(USER_ID):$(GROUP_ID) mfycheng/protoc-gen-go
