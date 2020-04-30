// Copyright 2018 PingCAP, Inc.
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

package tests

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tempurl"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
)

type serverConfig struct {
	Name                string
	DataDir             string
	ClientURLs          string
	AdvertiseClientURLs string
	PeerURLs            string
	AdvertisePeerURLs   string
	ClusterConfig       *clusterConfig
}

func newServerConfig(name string, cc *clusterConfig) *serverConfig {
	tempDir, _ := ioutil.TempDir("/tmp", "pd-tests")
	return &serverConfig{
		Name:          name,
		DataDir:       tempDir,
		ClientURLs:    tempurl.Alloc(),
		PeerURLs:      tempurl.Alloc(),
		ClusterConfig: cc,
	}
}

func (c *serverConfig) Generate(opts ...ConfigOption) (*config.Config, error) {
	arguments := []string{
		"--name=" + c.Name,
		"--data-dir=" + c.DataDir,
		"--client-urls=" + c.ClientURLs,
		"--advertise-client-urls=" + c.AdvertiseClientURLs,
		"--peer-urls=" + c.PeerURLs,
		"--advertise-peer-urls=" + c.AdvertisePeerURLs,
	}

	arguments = append(arguments, "--initial-cluster="+c.ClusterConfig.GetServerAddrs())

	cfg := config.NewConfig()
	err := cfg.Parse(arguments)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg, nil
}

type clusterConfig struct {
	InitialServers []*serverConfig
}

func newClusterConfig(n int) *clusterConfig {
	var cc clusterConfig
	for i := 0; i < n; i++ {
		c := newServerConfig(cc.nextServerName(), &cc)
		cc.InitialServers = append(cc.InitialServers, c)
	}
	return &cc
}

func (c *clusterConfig) nextServerName() string {
	return fmt.Sprintf("pd%d", len(c.InitialServers)+1)
}

func (c *clusterConfig) GetServerAddrs() string {
	var addrs []string
	for _, s := range c.InitialServers {
		addrs = append(addrs, fmt.Sprintf("%s=%s", s.Name, s.PeerURLs))
	}
	return strings.Join(addrs, ",")
}

func (c *clusterConfig) GetClientURLs() string {
	return c.InitialServers[0].ClientURLs
}
