#!/usr/bin/env bash
set -e

echo "" > coverage.txt

golangci-lint run --exclude horizon.Problem --exclude horizon.Transaction --exclude github.com/golang/protobuf/proto

for d in $(go list -e ./... | grep -v vendor | grep -v mocks | grep -v systemtest); do
    go test -test.v=true -race -coverprofile=profile.out $d
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        rm profile.out
    fi
done
