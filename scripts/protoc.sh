#!/usr/bin/env bash
protoc --proto_path=stream stream/stream.proto --go_out=plugins=grpc,paths=source_relative:./stream
protoc --proto_path=test test/test.proto --go_out=plugins=grpc,paths=source_relative:./test
