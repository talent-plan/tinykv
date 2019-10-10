GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GOBUILD := go build $(BUILD_FLAG)

default:
	$(GOBUILD) -ldflags "-X main.gitHash=`git rev-parse HEAD`" -o bin/unistore-server unistore-server/main.go

linux:
	GOOS=linux $(GOBUILD) -ldflags "-X main.gitHash=`git rev-parse HEAD`" -o bin/unistore-server-linux unistore-server/main.go
