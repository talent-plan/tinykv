#!/bin/bash

# You might need to go get -v github.com/gogo/protobuf/...

protos=${GOPATH-$HOME/go}/src/github.com/Connor1996/badger/protos
pushd $protos > /dev/null
protoc --gofast_out=plugins=grpc:. -I=. *.proto
