module github.com/kinecosystem/agora

go 1.13

require (
	cirello.io/dynamolock v1.3.3
	cloud.google.com/go/bigquery v1.14.0
	github.com/aws/aws-sdk-go v1.25.25
	github.com/aws/aws-sdk-go-v2 v0.17.0
	github.com/envoyproxy/protoc-gen-validate v0.1.0
	github.com/go-redis/redis/v7 v7.0.0
	github.com/go-redis/redis_rate/v8 v8.0.0
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/golang-lru v0.5.1
	github.com/jackc/pgx/v4 v4.9.2
	github.com/kinecosystem/agora-api v0.25.0
	github.com/kinecosystem/agora-common v0.70.0
	github.com/kinecosystem/go v0.0.0-20191108204735-d6832148266e
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/mr-tron/base58 v1.2.0
	github.com/ory/dockertest v3.3.5+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cobra v1.1.1
	github.com/stellar/go v0.0.0-20191211203732-552e507ffa37
	github.com/stretchr/testify v1.5.1
	github.com/x-cray/logrus-prefixed-formatter v0.5.2
	github.com/ybbus/jsonrpc v2.1.2+incompatible
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/api v0.36.0
	google.golang.org/grpc v1.33.2
	gotest.tools v2.2.0+incompatible
)

// This dependency of stellar/go no longer exists; use a forked version of the repo instead.
replace bitbucket.org/ww/goautoneg => github.com/adjust/goautoneg v0.0.0-20150426214442-d788f35a0315
