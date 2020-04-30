// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tempurl"
	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestKV(t *testing.T) {
	TestingT(t)
}

type testEtcdKVSuite struct{}

var _ = Suite(&testEtcdKVSuite{})

func (s *testEtcdKVSuite) TestEtcdKV(c *C) {
	cfg := newTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	c.Assert(err, IsNil)

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	c.Assert(err, IsNil)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))

	kv := NewEtcdKVBase(client, rootPath)

	keys := []string{"test/key1", "test/key2", "test/key3", "test/key4", "test/key5"}
	vals := []string{"val1", "val2", "val3", "val4", "val5"}

	v, err := kv.Load(keys[0])
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")

	for i := range keys {
		err = kv.Save(keys[i], vals[i])
		c.Assert(err, IsNil)
	}
	for i := range keys {
		v, err = kv.Load(keys[i])
		c.Assert(err, IsNil)
		c.Assert(v, Equals, vals[i])
	}
	ks, vs, err := kv.LoadRange(keys[0], "test/zzz", 100)
	c.Assert(err, IsNil)
	c.Assert(ks, DeepEquals, keys)
	c.Assert(vs, DeepEquals, vals)
	ks, vs, err = kv.LoadRange(keys[0], "test/zzz", 3)
	c.Assert(err, IsNil)
	c.Assert(ks, DeepEquals, keys[:3])
	c.Assert(vs, DeepEquals, vals[:3])
	ks, vs, err = kv.LoadRange(keys[0], keys[3], 100)
	c.Assert(err, IsNil)
	c.Assert(ks, DeepEquals, keys[:3])
	c.Assert(vs, DeepEquals, vals[:3])

	v, err = kv.Load(keys[1])
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "val2")
	c.Assert(kv.Remove(keys[1]), IsNil)
	v, err = kv.Load(keys[1])
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")

	etcd.Close()
	cleanConfig(cfg)
}

func newTestSingleConfig() *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir, _ = ioutil.TempDir("/tmp", "test_etcd")
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

func cleanConfig(cfg *embed.Config) {
	// Clean data directory
	os.RemoveAll(cfg.Dir)
}
