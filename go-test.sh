#!/usr/bin/env bash
set -e

echo "" > coverage.txt

for d in $(go list -e ./... | grep -v vendor | grep -v mocks | grep -v systemtest); do
    go vet $d
    go test -test.v=true -race -coverprofile=profile.out $d
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        rm profile.out
    fi
done
