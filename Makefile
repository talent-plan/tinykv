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
	LOG_LEVEL=fatal $(GOTEST) -cover $(PACKAGES)

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
	$(GOTEST) ./kv/server -run 1

project2: project2a project2b project2c

project2a:
	$(GOTEST) ./raft -run 2A

project2aa:
	$(GOTEST) ./raft -run 2AA

project2ab:
	$(GOTEST) ./raft -run 2AB

project2ac:
	$(GOTEST) ./raft -run 2AC

project2b:
	$(GOTEST) ./kv/test_raftstore -run ^TestBasic2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConcurrent2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestUnreliable2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOnePartition2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestManyPartitionsOneClient2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestManyPartitionsManyClients2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistOneClient2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistConcurrent2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistConcurrentUnreliable2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistPartition2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistPartitionUnreliable2B$ || true

project2c:
	$(GOTEST) ./raft ./kv/test_raftstore -run 2C

project3: project3a project3b project3c

project3a:
	$(GOTEST) ./raft -run 3A

project3b:
	$(GOTEST) ./kv/test_raftstore -run ^TestTransferLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestBasicConfChange3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRecoverManyClients3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliable3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitRecoverManyClients3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitUnreliable3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true

project3c:
	$(GOTEST) ./scheduler/server ./scheduler/server/schedulers -check.f="3C"

project4: project4a project4b project4c

project4a:
	$(GOTEST) ./kv/transaction/... -run 4A

project4b:
	$(GOTEST) ./kv/transaction/... -run 4B

project4c:
	$(GOTEST) ./kv/transaction/... -run 4C
