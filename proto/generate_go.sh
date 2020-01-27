#!/usr/bin/env bash

# This script generates Go representations of Protobuf protocols. It will generate Go code in the pkg subdirectory
# for every protocol in the proto subdirectory. It uses protoc, the protobuf compiler, which must be installed.

set -ex

push () {
    pushd $1 >/dev/null 2>&1
}

pop () {
    popd $1 >/dev/null 2>&1
}

cmd_exists () {
    which "$1" 1>/dev/null 2>&1
}

PROGRAM=$(basename "$0")

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi

GO_PREFIX_PATH=github.com/pingcap-incubator/tinykv/proto/pkg
export PATH=$(pwd)/_tools/bin:$GOPATH/bin:$PATH

echo "install tools..."
GO111MODULE=off go get github.com/twitchtv/retool
# Ensure we're using the right versions of our tools (see tools.json).
GO111MODULE=off retool -base-dir=$(pwd) sync || exit 1

function collect() {
    file=$(basename $1)
    base_name=$(basename $file ".proto")
    mkdir -p ../pkg/$base_name
    if [ -z $GO_OUT_M ]; then
        GO_OUT_M="M$file=$GO_PREFIX_PATH/$base_name"
    else
        GO_OUT_M="$GO_OUT_M,M$file=$GO_PREFIX_PATH/$base_name"
    fi
}

cd proto
for file in `ls *.proto`
    do
    collect $file
done

echo "generate go code..."
ret=0

function gen() {
    base_name=$(basename $1 ".proto")
    protoc -I.:../include --gofast_out=plugins=grpc,$GO_OUT_M:../pkg/$base_name $1 || ret=$?
    cd ../pkg/$base_name
    sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
    sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
    sed -i.bak -E 's/import io \"io\"//g' *.pb.go
    sed -i.bak -E 's/import math \"math\"//g' *.pb.go
    sed -i.bak -E 's/import _ \".*rustproto\"//' *.pb.go
    rm -f *.bak
    goimports -w *.pb.go
    cd ../../proto
}

for file in `ls *.proto`
    do
    gen $file
done
exit $ret
