// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"io/ioutil"
	"os"

	"github.com/pingcap-incubator/tinykv/kv/util/log"
)

func SetLogger(l *DefaultLogger) { raftLogger = l }

var (
	defaultLogger = &DefaultLogger{Logger: log.NewLogger(os.Stderr, "raft")}
	discardLogger = &DefaultLogger{Logger: log.NewLogger(ioutil.Discard, "")}
	raftLogger    = defaultLogger
)

const (
	calldepth = 2
)

// DefaultLogger is a default implementation of the Logger interface.
type DefaultLogger struct {
	*log.Logger
}

func (l *DefaultLogger) EnableTimestamps() {
	l.SetFlags(l.Flags() | log.Ldate | log.Ltime)
}

func (l *DefaultLogger) EnableDebug() {
	l.SetLevel(log.LOG_LEVEL_DEBUG)
}
