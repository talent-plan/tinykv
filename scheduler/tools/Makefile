all: bin/govet bin/overalls bin/golangci-lint bin/revive bin/failpoint-ctl bin/goimports

bin/govet:
	go build -o $@ github.com/dnephin/govet

bin/overalls:
	go build -o $@ github.com/go-playground/overalls

bin/golangci-lint:
	go build -o $@ github.com/golangci/golangci-lint/cmd/golangci-lint

bin/revive:
	go build -o $@ github.com/mgechev/revive

bin/failpoint-ctl:
	go build -o $@ github.com/pingcap/failpoint/failpoint-ctl

bin/goimports:
	go build -o $@ golang.org/x/tools/cmd/goimports
