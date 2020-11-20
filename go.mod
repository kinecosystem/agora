module github.com/kinecosystem/agora

go 1.13

require (
	cirello.io/dynamolock v1.3.3
	github.com/aws/aws-sdk-go v1.25.25
	github.com/aws/aws-sdk-go-v2 v0.17.0
	github.com/envoyproxy/protoc-gen-validate v0.1.0
	github.com/go-redis/redis/v7 v7.0.0
	github.com/go-redis/redis_rate/v8 v8.0.0
	github.com/golang/protobuf v1.4.2
	github.com/kinecosystem/agora-api v0.23.0
	github.com/kinecosystem/agora-common v0.44.0
	github.com/kinecosystem/go v0.0.0-20191108204735-d6832148266e
	github.com/mr-tron/base58 v1.2.0
	github.com/ory/dockertest v3.3.5+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/sirupsen/logrus v1.4.2
	github.com/stellar/go v0.0.0-20191211203732-552e507ffa37
	github.com/stretchr/testify v1.5.1
	github.com/ybbus/jsonrpc v2.1.2+incompatible
	golang.org/x/crypto v0.0.0-20191112222119-e1110fd1c708
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc v1.28.1
	google.golang.org/protobuf v1.23.0
)

// This dependency of stellar/go no longer exists; use a forked version of the repo instead.
replace bitbucket.org/ww/goautoneg => github.com/adjust/goautoneg v0.0.0-20150426214442-d788f35a0315
