// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
)

// Config number limitations
const (
	// DefTxnTotalSizeLimit is the default value of TxnTxnTotalSizeLimit.
	DefTxnTotalSizeLimit = 1024 * 1024 * 1024
)

// Valid config maps
var (
	ValidStorage = map[string]bool{
		"mocktikv": true,
		"tikv":     true,
	}

	CheckTableBeforeDrop = false
	// checkBeforeDropLDFlag is a go build flag.
	checkBeforeDropLDFlag = "None"
)

// Config contains configuration options.
type Config struct {
	Host             string `toml:"host" json:"host"`
	AdvertiseAddress string `toml:"advertise-address" json:"advertise-address"`
	Port             uint   `toml:"port" json:"port"`
	Cors             string `toml:"cors" json:"cors"`
	Store            string `toml:"store" json:"store"`
	Path             string `toml:"path" json:"path"`
	Lease            string `toml:"lease" json:"lease"`
	Log              Log    `toml:"log" json:"log"`
	Status           Status `toml:"status" json:"status"`
}

// Log is the log section of config.
type Log struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// File log config.
	File logutil.FileLogConfig `toml:"file" json:"file"`
}

// The ErrConfigValidationFailed error is used so that external callers can do a type assertion
// to defer handling of this specific error when someone does not want strict type checking.
// This is needed only because logging hasn't been set up at the time we parse the config file.
// This should all be ripped out once strict config checking is made the default behavior.
type ErrConfigValidationFailed struct {
	confFile       string
	UndecodedItems []string
}

func (e *ErrConfigValidationFailed) Error() string {
	return fmt.Sprintf("config file %s contained unknown configuration options: %s", e.confFile, strings.Join(e.UndecodedItems, ", "))
}

// Status is the status section of the config.
type Status struct {
	StatusHost string `toml:"status-host" json:"status-host"`

	StatusPort uint `toml:"status-port" json:"status-port"`

	ReportStatus bool `toml:"report-status" json:"report-status"`
}

var defaultConf = Config{
	Host:             "0.0.0.0",
	AdvertiseAddress: "",
	Port:             4000,
	Cors:             "",
	Store:            "mocktikv",
	Path:             "/tmp/tinysql",
	Lease:            "45s",
	Log: Log{
		Level: "info",
		File:  logutil.NewFileLogConfig(logutil.DefaultLogMaxSize),
	},
	Status: Status{
		ReportStatus: true,
		StatusHost:   "0.0.0.0",
		StatusPort:   10080,
	},
}

var (
	globalConf = atomic.Value{}
)

// NewConfig creates a new config instance with default value.
func NewConfig() *Config {
	conf := defaultConf
	return &conf
}

// GetGlobalConfig returns the global configuration for this server.
// It should store configuration from command line and configuration file.
// Other parts of the system can read the global configuration use this function.
func GetGlobalConfig() *Config {
	return globalConf.Load().(*Config)
}

// StoreGlobalConfig stores a new config to the globalConf. It mostly uses in the test to avoid some data races.
func StoreGlobalConfig(config *Config) {
	globalConf.Store(config)
}

// Load loads config options from a toml file.
func (c *Config) Load(confFile string) error {
	metaData, err := toml.DecodeFile(confFile, c)
	// If any items in confFile file are not mapped into the Config struct, issue
	// an error and stop the server from starting.
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 && err == nil {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		err = &ErrConfigValidationFailed{confFile, undecodedItems}
	}

	return err
}

// ToLogConfig converts *Log to *logutil.LogConfig.
func (l *Log) ToLogConfig() *logutil.LogConfig {
	return logutil.NewLogConfig(l.Level, "test", l.File, false, func(config *zaplog.Config) { config.DisableErrorVerbose = false })
}

func init() {
	globalConf.Store(&defaultConf)
	if checkBeforeDropLDFlag == "1" {
		CheckTableBeforeDrop = true
	}
}
