#!/usr/bin/env bash
set -e

echo "" > coverage.txt

golangci-lint run \
    --exclude horizon.Account \
    --exclude horizon.Problem \
    --exclude horizon.Signer \
    --exclude horizon.Transaction \
    --exclude proto.MessageName \
    --exclude github.com/golang/protobuf/proto

go test -test.v=true -race -coverprofile=profile.out $(go list -e ./...)
if [ -f profile.out ]; then
    cat profile.out >> coverage.txt
    rm profile.out
fi
