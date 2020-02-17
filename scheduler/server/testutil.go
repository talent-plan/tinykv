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

package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tempurl"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/typeutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"go.etcd.io/etcd/embed"

	// Register schedulers
	_ "github.com/pingcap-incubator/tinykv/scheduler/server/schedulers"
)

// CleanupFunc closes test pd server(s) and deletes any files left behind.
type CleanupFunc func()

// NewTestServer creates a pd server for testing.
func NewTestServer(c *check.C) (*Server, CleanupFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := NewTestSingleConfig(c)
	s, err := CreateServer(cfg)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if err = s.Run(ctx); err != nil {
		cancel()
		return nil, nil, err
	}

	cleanup := func() {
		cancel()
		s.Close()
		testutil.CleanServer(cfg)
	}
	return s, cleanup, nil
}

var zapLogOnce sync.Once

// NewTestSingleConfig is only for test to create one pd.
// Because PD client also needs this, so export here.
func NewTestSingleConfig(c *check.C) *config.Config {
	cfg := &config.Config{
		Name:       "pd",
		ClientUrls: tempurl.Alloc(),
		PeerUrls:   tempurl.Alloc(),

		InitialClusterState: embed.ClusterStateFlagNew,

		LeaderLease:     1,
		TsoSaveInterval: typeutil.NewDuration(200 * time.Millisecond),
	}

	cfg.AdvertiseClientUrls = cfg.ClientUrls
	cfg.AdvertisePeerUrls = cfg.PeerUrls
	cfg.DataDir, _ = ioutil.TempDir("/tmp", "test_pd")
	cfg.InitialCluster = fmt.Sprintf("pd=%s", cfg.PeerUrls)
	cfg.DisableStrictReconfigCheck = true
	cfg.TickInterval = typeutil.NewDuration(100 * time.Millisecond)
	cfg.ElectionInterval = typeutil.NewDuration(3 * time.Second)
	cfg.LeaderPriorityCheckInterval = typeutil.NewDuration(100 * time.Millisecond)
	err := cfg.SetupLogger()
	c.Assert(err, check.IsNil)
	zapLogOnce.Do(func() {
		log.ReplaceGlobals(cfg.GetZapLogger(), cfg.GetZapLogProperties())
	})

	c.Assert(cfg.Adjust(nil), check.IsNil)

	return cfg
}

// NewTestMultiConfig is only for test to create multiple pd configurations.
// Because PD client also needs this, so export here.
func NewTestMultiConfig(c *check.C, count int) []*config.Config {
	cfgs := make([]*config.Config, count)

	var clusters []string
	for i := 1; i <= count; i++ {
		cfg := NewTestSingleConfig(c)
		cfg.Name = fmt.Sprintf("pd%d", i)

		clusters = append(clusters, fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls))

		cfgs[i-1] = cfg
	}

	initialCluster := strings.Join(clusters, ",")
	for _, cfg := range cfgs {
		cfg.InitialCluster = initialCluster
	}

	return cfgs
}
