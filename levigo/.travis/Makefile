all: test

LEVELDB_VERSION ?= v1.18
SNAPPY_VERSION ?= 1.1.7

export CFLAGS ?= -I$(PWD)/root/snappy-$(SNAPPY_VERSION)/include
export CXXFLAGS ?= -I$(PWD)/root/snappy-$(SNAPPY_VERSION)/build
export LDFLAGS ?= -L$(PWD)/root/snappy-$(SNAPPY_VERSION)/build
export CGO_CFLAGS ?= -I$(PWD)/root/snappy-$(SNAPPY_VERSION)/build -I$(PWD)/root/leveldb/include
export CGO_LDFLAGS ?= -L$(PWD)/root/snappy-$(SNAPPY_VERSION)/build -L$(PWD)/root/leveldb -lsnappy
export GOPATH ?= $(PWD)/root/go
export LD_LIBRARY_PATH := $(PWD)/root/snappy-$(SNAPPY_VERSION)/build:$(PWD)/root/leveldb:$(LD_LIBRARY_PATH)

archives/snappy-$(SNAPPY_VERSION).tar.gz: archives
	curl -L https://github.com/google/snappy/archive/$(SNAPPY_VERSION).tar.gz > $@

archives:
	mkdir -v archives

levigo: root/snappy-$(SNAPPY_VERSION)/STAMP root/leveldb/STAMP
	cd ../ && go get -d .
	cd ../ && go build .
	cd ../ && go test -test.v=true .

root:
	mkdir -v root

root/leveldb: root
	cd root && git clone https://github.com/google/leveldb.git

root/leveldb/STAMP: root/leveldb root/snappy-$(SNAPPY_VERSION)/STAMP
	cd root/leveldb && git checkout $(LEVELDB_VERSION)
	$(MAKE) -C root/leveldb
	touch $@

root/snappy-$(SNAPPY_VERSION): archives/snappy-$(SNAPPY_VERSION).tar.gz root
	tar xzvf archives/snappy-$(SNAPPY_VERSION).tar.gz -C root

root/snappy-$(SNAPPY_VERSION)/STAMP: root/snappy-$(SNAPPY_VERSION)
	mkdir -p root/snappy-$(SNAPPY_VERSION)/build && cd root/snappy-$(SNAPPY_VERSION)/build && cmake -DCMAKE_INSTALL_PREFIX:PATH=$(pwd) ../ && make
	touch $@

test: levigo

clean:
	-rm -rf archives
	-rm -rf root

.PHONY: levigo test
