// Copyright 2016 PingCAP, Inc.
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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/docker/go-units"
	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
)

var _ = Suite(&testStoreSuite{})

type testStoreSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
	stores    []*metapb.Store
}

func requestStatusBody(c *C, client *http.Client, method string, url string) (int, []byte) {
	req, err := http.NewRequest(method, url, nil)
	c.Assert(err, IsNil)
	resp, err := client.Do(req)
	c.Assert(err, IsNil)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	err = resp.Body.Close()
	c.Assert(err, IsNil)
	return resp.StatusCode, body
}

func (s *testStoreSuite) SetUpSuite(c *C) {
	s.stores = []*metapb.Store{
		{
			// metapb.StoreState_Up == 0
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			Id:      4,
			Address: "tikv4",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			// metapb.StoreState_Offline == 1
			Id:      6,
			Address: "tikv6",
			State:   metapb.StoreState_Offline,
			Version: "2.0.0",
		},
		{
			// metapb.StoreState_Tombstone == 2
			Id:      7,
			Address: "tikv7",
			State:   metapb.StoreState_Tombstone,
			Version: "2.0.0",
		},
	}

	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)

	for _, store := range s.stores {
		mustPutStore(c, s.svr, store.Id, store.State, nil)
	}
}

func (s *testStoreSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func checkStoresInfo(c *C, ss []*StoreInfo, want []*metapb.Store) {
	c.Assert(len(ss), Equals, len(want))
	mapWant := make(map[uint64]*metapb.Store)
	for _, s := range want {
		if _, ok := mapWant[s.Id]; !ok {
			mapWant[s.Id] = s
		}
	}
	for _, s := range ss {
		c.Assert(s.Store.Store, DeepEquals, mapWant[s.Store.Store.Id])
	}
}

func (s *testStoreSuite) TestStoresList(c *C) {
	url := fmt.Sprintf("%s/stores", s.urlPrefix)
	info := new(StoresInfo)
	err := readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, info.Stores, s.stores[:3])

	url = fmt.Sprintf("%s/stores?state=0", s.urlPrefix)
	info = new(StoresInfo)
	err = readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, info.Stores, s.stores[:2])

	url = fmt.Sprintf("%s/stores?state=1", s.urlPrefix)
	info = new(StoresInfo)
	err = readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, info.Stores, s.stores[2:3])

}

func (s *testStoreSuite) TestStoreGet(c *C) {
	url := fmt.Sprintf("%s/store/1", s.urlPrefix)
	s.svr.StoreHeartbeat(
		context.Background(), &pdpb.StoreHeartbeatRequest{
			Header: &pdpb.RequestHeader{ClusterId: s.svr.ClusterID()},
			Stats: &pdpb.StoreStats{
				StoreId:   1,
				Capacity:  1798985089024,
				Available: 1709868695552,
				UsedSize:  85150956358,
			},
		},
	)

	info := new(StoreInfo)
	err := readJSONWithURL(url, info)

	c.Assert(err, IsNil)
	capacity, _ := units.RAMInBytes("1.636TiB")
	available, _ := units.RAMInBytes("1.555TiB")
	c.Assert(int64(info.Status.Capacity), Equals, capacity)
	c.Assert(int64(info.Status.Available), Equals, available)
	checkStoresInfo(c, []*StoreInfo{info}, s.stores[:1])
}

func (s *testStoreSuite) TestStoreLabel(c *C) {
	url := fmt.Sprintf("%s/store/1", s.urlPrefix)
	var info StoreInfo
	err := readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.Labels, HasLen, 0)

	// Test merge.
	// enable label match check.
	labelCheck := map[string]string{"strictly-match-label": "true"}
	lc, _ := json.Marshal(labelCheck)
	err = postJSON(s.urlPrefix+"/config", lc)
	c.Assert(err, IsNil)
	// Test set.
	labels := map[string]string{"zone": "cn", "host": "local"}
	b, err := json.Marshal(labels)
	c.Assert(err, IsNil)
	err = postJSON(url+"/label", b)
	c.Assert(strings.Contains(err.Error(), "key matching the label was not found"), IsTrue)
	locationLabels := map[string]string{"location-labels": "zone,host"}
	ll, _ := json.Marshal(locationLabels)
	err = postJSON(s.urlPrefix+"/config", ll)
	c.Assert(err, IsNil)
	err = postJSON(url+"/label", b)
	c.Assert(err, IsNil)

	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.Labels, HasLen, len(labels))
	for _, l := range info.Store.Labels {
		c.Assert(labels[l.Key], Equals, l.Value)
	}

	// Test merge.
	// disable label match check.
	labelCheck = map[string]string{"strictly-match-label": "false"}
	lc, _ = json.Marshal(labelCheck)
	err = postJSON(s.urlPrefix+"/config", lc)
	c.Assert(err, IsNil)

	labels = map[string]string{"zack": "zack1", "Host": "host1"}
	b, err = json.Marshal(labels)
	c.Assert(err, IsNil)
	err = postJSON(url+"/label", b)
	c.Assert(err, IsNil)

	expectLabel := map[string]string{"zone": "cn", "zack": "zack1", "host": "host1"}
	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.Labels, HasLen, len(expectLabel))
	for _, l := range info.Store.Labels {
		c.Assert(expectLabel[l.Key], Equals, l.Value)
	}

	s.stores[0].Labels = info.Store.Labels
}

func (s *testStoreSuite) TestStoreDelete(c *C) {
	table := []struct {
		id     int
		status int
	}{
		{
			id:     6,
			status: http.StatusOK,
		},
		{
			id:     7,
			status: http.StatusGone,
		},
	}
	client := newHTTPClient()
	for _, t := range table {
		url := fmt.Sprintf("%s/store/%d", s.urlPrefix, t.id)
		status, _ := requestStatusBody(c, client, http.MethodDelete, url)
		c.Assert(status, Equals, t.status)
	}
}

func (s *testStoreSuite) TestStoreSetState(c *C) {
	url := fmt.Sprintf("%s/store/1", s.urlPrefix)
	info := StoreInfo{}
	err := readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.State, Equals, metapb.StoreState_Up)

	// Set to Offline.
	info = StoreInfo{}
	err = postJSON(url+"/state?state=Offline", nil)
	c.Assert(err, IsNil)
	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.State, Equals, metapb.StoreState_Offline)

	// Invalid state.
	info = StoreInfo{}
	err = postJSON(url+"/state?state=Foo", nil)
	c.Assert(err, NotNil)
	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.State, Equals, metapb.StoreState_Offline)

	// Set back to Up.
	info = StoreInfo{}
	err = postJSON(url+"/state?state=Up", nil)
	c.Assert(err, IsNil)
	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.State, Equals, metapb.StoreState_Up)
}

func (s *testStoreSuite) TestUrlStoreFilter(c *C) {
	table := []struct {
		u    string
		want []*metapb.Store
	}{
		{
			u:    "http://localhost:2379/pd/api/v1/stores",
			want: s.stores[:3],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2",
			want: s.stores[3:],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=0",
			want: s.stores[:2],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2&state=1",
			want: s.stores[2:],
		},
	}

	for _, t := range table {
		uu, err := url.Parse(t.u)
		c.Assert(err, IsNil)
		f, err := newStoreStateFilter(uu)
		c.Assert(err, IsNil)
		c.Assert(f.filter(s.stores), DeepEquals, t.want)
	}

	u, err := url.Parse("http://localhost:2379/pd/api/v1/stores?state=foo")
	c.Assert(err, IsNil)
	_, err = newStoreStateFilter(u)
	c.Assert(err, NotNil)

	u, err = url.Parse("http://localhost:2379/pd/api/v1/stores?state=999999")
	c.Assert(err, IsNil)
	_, err = newStoreStateFilter(u)
	c.Assert(err, NotNil)
}

func (s *testStoreSuite) TestDownState(c *C) {
	store := core.NewStoreInfo(
		&metapb.Store{
			State: metapb.StoreState_Up,
		},
		core.SetStoreStats(&pdpb.StoreStats{}),
		core.SetLastHeartbeatTS(time.Now()),
	)
	storeInfo := newStoreInfo(s.svr.GetScheduleConfig(), store)
	c.Assert(storeInfo.Store.StateName, Equals, metapb.StoreState_Up.String())

	newStore := store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Minute * 2)))
	storeInfo = newStoreInfo(s.svr.GetScheduleConfig(), newStore)
	c.Assert(storeInfo.Store.StateName, Equals, disconnectedName)

	newStore = store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Hour * 2)))
	storeInfo = newStoreInfo(s.svr.GetScheduleConfig(), newStore)
	c.Assert(storeInfo.Store.StateName, Equals, downStateName)
}
