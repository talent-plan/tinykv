#!/bin/bash
cd node
for subpath in $(echo $GOPATH | tr ":" "\n")
do
  if [ -d $subpath"/src/github.com/pingcap/tidb" ]; then
  	mv $subpath"/src/github.com/pingcap/tidb/vendor" $subpath"/src/github.com/pingcap/tidb/_vendor"
  	go build
  	mv $subpath"/src/github.com/pingcap/tidb/_vendor" $subpath"/src/github.com/pingcap/tidb/vendor"
  fi
done
