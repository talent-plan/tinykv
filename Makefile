SHELL := /bin/bash
PROJECT=tinykv
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO                  := GO111MODULE=on go
GOBUILD             := $(GO) build $(BUILD_FLAG) -tags codes
GOTEST              := $(GO) test -v --count=1 --parallel=1 -p=1

TEST_LDFLAGS        := ""

PACKAGE_LIST        := go list ./...| grep -vE "cmd"
PACKAGES            := $$($(PACKAGE_LIST))

# Targets
.PHONY: clean test proto kv scheduler dev

default: kv scheduler

dev: default test

test:
	@echo "Running tests in native mode."
	@export TZ='Asia/Shanghai'; \
	$(GOTEST) -cover $(PACKAGES)

CURDIR := $(shell pwd)
export PATH := $(CURDIR)/bin/:$(PATH)
proto:
	mkdir -p $(CURDIR)/bin
	(cd proto && ./generate_go.sh)
	GO111MODULE=on go build ./proto/pkg/...

kv:
	$(GOBUILD) -o bin/tinykv-server kv/main.go

scheduler:
	$(GOBUILD) -o bin/tinyscheduler-server scheduler/main.go

ci: default
	@echo "Checking formatting"
	@test -z "$$(gofmt -s -l $$(find . -name '*.go' -type f -print) | tee /dev/stderr)"
	@echo "Running Go vet"
	@go vet ./...

format:
	@gofmt -s -w `find . -name '*.go' -type f ! -path '*/_tools/*' -print`

project1:
	go test -count=1 ./kv/server -run 1 

project2: project2a project2b project2c

project2a:
	go test -count=1 ./raft -run 2A

project2aa:
	go test -count=1 ./raft -run 2AA

project2ab:
	go test -count=1 ./raft -run 2AB

project2ac:
	go test -count=1 ./raft -run 2AC

project2b:
	go test -count=1 ./kv/test_raftstore -run 2B

project2c:
	go test -count=1 ./raft ./kv/test_raftstore -run 2C

project3: project3a project3b project3c

project3a:
	go test -count=1 ./raft -run 3A

project3b:
	go test -count=1 ./raft -run 3B

project3c:
	go test -count=1 ./scheduler/... -run 3C

project4: project4a project4b project4c

project4a:
	go test -count=1 ./kv/transaction/... -run 4A

project4b:
	go test -count=1 ./kv/transaction/... -run 4B

project4c:
	go test -count=1 ./kv/transaction/... -run 4C
