#!/usr/bin/env bash
set -euo pipefail

# This script generates tools.json
# It helps record what releases/branches are being used

cd $(dirname "$0")/..
which retool >/dev/null || go get github.com/twitchtv/retool

# tool environment
# check runner
./scripts/retool add gopkg.in/alecthomas/gometalinter.v2 v2.0.5
# check spelling
./scripts/retool add github.com/client9/misspell/cmd/misspell v0.3.4
# checks correctness
./scripts/retool add github.com/gordonklaus/ineffassign 7bae11eba15a3285c75e388f77eb6357a2d73ee2
./scripts/retool add honnef.co/go/tools/cmd/megacheck master
./scripts/retool add github.com/dnephin/govet 4a96d43e39d340b63daa8bc5576985aa599885f6
# slow checks
./scripts/retool add github.com/kisielk/errcheck v1.1.0
# linter
./scripts/retool add github.com/mgechev/revive 7773f47324c2bf1c8f7a5500aff2b6c01d3ed73b
./scripts/retool add github.com/securego/gosec/cmd/gosec 1.0.0
# go fail
./scripts/retool add github.com/pingcap/failpoint/failpoint-ctl master
# deadlock detection
./scripts/retool add golang.org/x/tools/cmd/goimports 04b5d21e00f1f47bd824a6ade581e7189bacde87
