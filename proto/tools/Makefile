all: bin/goimports bin/protoc-gen-gofast

bin/goimports:
	go build -o $@ golang.org/x/tools/cmd/goimports

bin/protoc-gen-gofast:
	go build -o $@ github.com/gogo/protobuf/protoc-gen-gofast