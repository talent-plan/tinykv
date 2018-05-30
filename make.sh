#!/bin/bash
cd node
for subpath in $(echo $GOPATH | tr ":" "\n")
do
  if [ -d $subpath"/src/github.com/pingcap/tidb" ]; then
  	mv $subpath"/src/github.com/pingcap/tidb/vendor" $subpath"/src/github.com/pingcap/tidb/_vendor"
  	go build -ldflags "-X main.gitHash=`git rev-parse HEAD`"
  	mv $subpath"/src/github.com/pingcap/tidb/_vendor" $subpath"/src/github.com/pingcap/tidb/vendor"
  fi
done
