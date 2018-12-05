GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO      := GO111MODULE=on go
GOBUILD := $(GO) build $(BUILD_FLAG)

default:
	$(GOBUILD) -ldflags "-X main.gitHash=`git rev-parse HEAD`" -o node/node node/main.go
