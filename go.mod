module github.com/kinecosystem/agora-transaction-services-internal

go 1.13

require (
	github.com/aws/aws-sdk-go-v2 v0.17.0
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.3.2
	github.com/kinecosystem/agora-common v0.19.0
	github.com/kinecosystem/go v0.0.0-20191108204735-d6832148266e
	github.com/kinecosystem/kin-api v0.3.0
	github.com/ory/dockertest v3.3.5+incompatible
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.2
	github.com/stellar/go v0.0.0-20191211203732-552e507ffa37
	github.com/stretchr/testify v1.4.0
	google.golang.org/grpc v1.25.1
)

replace github.com/kinecosystem/kin-api => ../kin-api

replace github.com/kinecosystem/agora-common => ../agora-common-internal
