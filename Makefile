PROJECT=unistore
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO                  := GO111MODULE=on go
GOBUILD             := $(GO) build $(BUILD_FLAG) -tags codes
GOTEST              := $(GO) test -p 8

LDFLAGS             += -X "main.gitHash=`git rev-parse HEAD`" 
TEST_LDFLAGS        := ""

PACKAGE_LIST        := go list ./...| grep -vE "cmd"
PACKAGES            := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'

# Targets
.PHONY: build clean test proto

default: build

test:
	@echo "Running tests in native mode."
	@export TZ='Asia/Shanghai'; \
	$(GOTEST) -cover $(PACKAGES)

build:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/unistore-server unistore-server/main.go

linux:
	GOOS=linux $(GOBUILD) -ldflags "-X main.gitHash=`git rev-parse HEAD`" -o bin/unistore-server-linux unistore-server/main.go


CURDIR := $(shell pwd)
export PATH := $(CURDIR)/bin/:$(PATH)

proto:
	mkdir -p $(CURDIR)/bin
	./generate_go.sh
	GO111MODULE=on go build ./pkg/...