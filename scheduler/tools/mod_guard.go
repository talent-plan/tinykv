// +build modguard

package tools

import (
	_ "github.com/dnephin/govet"
	_ "github.com/go-playground/overalls"
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
	_ "github.com/mgechev/revive"
	_ "github.com/pingcap/failpoint/failpoint-ctl"
	_ "golang.org/x/tools/cmd/goimports"
)
