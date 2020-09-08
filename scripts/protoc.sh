#!/usr/bin/env bash

cd "$(dirname "$0")/.."

set -e

PROMETHEUS_PROTO_DIR="$(go list -f '{{ .Dir }}' -m github.com/prometheus/client_model)"

protoc --proto_path=stream --proto_path="$PROMETHEUS_PROTO_DIR" stream/stream.proto --go-grpc_out=requireUnimplementedServers=false,paths=source_relative:./stream --go_out=paths=source_relative:./stream
protoc --proto_path=test test/test.proto --go-grpc_out=requireUnimplementedServers=false,paths=source_relative:./test --go_out=paths=source_relative:./test
