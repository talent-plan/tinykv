FDB_CHECK := $(shell command -v fdbcli 2> /dev/null)
ROCKSDB_CHECK := $(shell echo "int main() { return 0; }" | gcc -lrocksdb -x c++ -o /dev/null - 2>/dev/null; echo $$?)
SQLITE_CHECK := $(shell echo "int main() { return 0; }" | gcc -lsqlite3 -x c++ -o /dev/null - 2>/dev/null; echo $$?)

TAGS =

ifdef FDB_CHECK
	TAGS += foundationdb
endif

ifneq ($(shell go env GOOS), $(shell go env GOHOSTOS))
	CROSS_COMPILE := 1
endif
ifneq ($(shell go env GOARCH), $(shell go env GOHOSTARCH))
	CROSS_COMPILE := 1
endif

ifndef CROSS_COMPILE

ifeq ($(SQLITE_CHECK), 0)
	TAGS += libsqlite3
endif

ifeq ($(ROCKSDB_CHECK), 0)
	TAGS += rocksdb
	CGO_CXXFLAGS := "${CGO_CXXFLAGS} -std=c++11"
	CGO_FLAGS += CGO_CXXFLAGS=$(CGO_CXXFLAGS)
endif

endif

default: build

build: export GO111MODULE=on
build:
	$(CGO_FLAGS) go build -o bin/go-ycsb cmd/go-ycsb/*

check:
	golint -set_exit_status db/... cmd/... pkg/...


