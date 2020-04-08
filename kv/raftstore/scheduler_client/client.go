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

package scheduler_client

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"google.golang.org/grpc"
)

// Client is a Scheduler client.
// It should not be used after calling Close().
type Client interface {
	GetClusterID(ctx context.Context) uint64
	AllocID(ctx context.Context) (uint64, error)
	Bootstrap(ctx context.Context, store *metapb.Store) (*schedulerpb.BootstrapResponse, error)
	IsBootstrapped(ctx context.Context) (bool, error)
	PutStore(ctx context.Context, store *metapb.Store) error
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error)
	GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error)
	AskSplit(ctx context.Context, region *metapb.Region) (*schedulerpb.AskSplitResponse, error)
	StoreHeartbeat(ctx context.Context, stats *schedulerpb.StoreStats) error
	RegionHeartbeat(*schedulerpb.RegionHeartbeatRequest) error
	SetRegionHeartbeatResponseHandler(storeID uint64, h func(*schedulerpb.RegionHeartbeatResponse))
	Close()
}

const (
	schedulerTimeout      = time.Second
	retryInterval         = time.Second
	maxInitClusterRetries = 100
	maxRetryCount         = 10
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied Scheduler addresses.
	errFailInitClusterID = errors.New("[scheduler] failed to get cluster id")
)

type client struct {
	urls      []string
	clusterID uint64
	tag       string

	connMu struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
		leader      string
	}
	checkLeaderCh chan struct{}

	receiveRegionHeartbeatCh chan *schedulerpb.RegionHeartbeatResponse
	regionCh                 chan *schedulerpb.RegionHeartbeatRequest
	pendingRequest           *schedulerpb.RegionHeartbeatRequest

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	heartbeatHandler atomic.Value
}

// NewClient creates a Scheduler client.
func NewClient(pdAddrs []string, tag string) (Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	urls := make([]string, 0, len(pdAddrs))
	for _, addr := range pdAddrs {
		if strings.Contains(addr, "://") {
			urls = append(urls, addr)
		} else {
			urls = append(urls, "http://"+addr)
		}
	}
	log.Infof("[%s][scheduler] create scheduler client with endpoints %v", tag, urls)

	c := &client{
		urls:                     urls,
		receiveRegionHeartbeatCh: make(chan *schedulerpb.RegionHeartbeatResponse, 1),
		checkLeaderCh:            make(chan struct{}, 1),
		ctx:                      ctx,
		cancel:                   cancel,
		tag:                      tag,
		regionCh:                 make(chan *schedulerpb.RegionHeartbeatRequest, 64),
	}
	c.connMu.clientConns = make(map[string]*grpc.ClientConn)

	var (
		err     error
		members *schedulerpb.GetMembersResponse
	)
	for i := 0; i < maxRetryCount; i++ {
		if members, err = c.updateLeader(); err == nil {
			break
		}
		time.Sleep(retryInterval)
	}
	if err != nil {
		return nil, err
	}

	c.clusterID = members.GetHeader().GetClusterId()
	log.Infof("[%s][scheduler] init cluster id %v", tag, c.clusterID)
	c.wg.Add(2)
	go c.checkLeaderLoop()
	go c.heartbeatStreamLoop()

	return c, nil
}

func (c *client) schedulerUpdateLeader() {
	select {
	case c.checkLeaderCh <- struct{}{}:
	default:
	}
}

func (c *client) checkLeaderLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-c.checkLeaderCh:
		case <-ticker.C:
		case <-ctx.Done():
			return
		}

		if _, err := c.updateLeader(); err != nil {
			log.Errorf("[scheduler] failed updateLeader, err: %s", err)
		}
	}
}

func (c *client) updateLeader() (*schedulerpb.GetMembersResponse, error) {
	for _, u := range c.urls {
		ctx, cancel := context.WithTimeout(c.ctx, schedulerTimeout)
		members, err := c.getMembers(ctx, u)
		cancel()
		if err != nil || members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0 {
			select {
			case <-c.ctx.Done():
				return nil, err
			default:
				continue
			}
		}

		c.updateURLs(members.GetMembers(), members.GetLeader())
		return members, c.switchLeader(members.GetLeader().GetClientUrls())
	}
	return nil, errors.Errorf("failed to get leader from %v", c.urls)
}

func (c *client) updateURLs(members []*schedulerpb.Member, leader *schedulerpb.Member) {
	urls := make([]string, 0, len(members))
	for _, m := range members {
		if m.GetMemberId() == leader.GetMemberId() {
			continue
		}
		urls = append(urls, m.GetClientUrls()...)
	}
	c.urls = append(urls, leader.GetClientUrls()...)
}

func (c *client) switchLeader(addrs []string) error {
	addr := addrs[0]

	c.connMu.RLock()
	oldLeader := c.connMu.leader
	c.connMu.RUnlock()

	if addr == oldLeader {
		return nil
	}

	log.Infof("[scheduler] switch leader, new-leader: %s, old-leader: %s", addr, oldLeader)
	if _, err := c.getOrCreateConn(addr); err != nil {
		return err
	}

	c.connMu.Lock()
	c.connMu.leader = addr
	c.connMu.Unlock()
	return nil
}

func (c *client) getMembers(ctx context.Context, url string) (*schedulerpb.GetMembersResponse, error) {
	cc, err := c.getOrCreateConn(url)
	if err != nil {
		return nil, err
	}
	return schedulerpb.NewSchedulerClient(cc).GetMembers(ctx, new(schedulerpb.GetMembersRequest))
}

func (c *client) getOrCreateConn(addr string) (*grpc.ClientConn, error) {
	c.connMu.RLock()
	conn, ok := c.connMu.clientConns[addr]
	c.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	cc, err := grpc.Dial(u.Host, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if old, ok := c.connMu.clientConns[addr]; ok {
		cc.Close()
		return old, nil
	}
	c.connMu.clientConns[addr] = cc
	return cc, nil
}

func (c *client) leaderClient() schedulerpb.SchedulerClient {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return schedulerpb.NewSchedulerClient(c.connMu.clientConns[c.connMu.leader])
}

func (c *client) doRequest(ctx context.Context, f func(context.Context, schedulerpb.SchedulerClient) error) error {
	var err error
	for i := 0; i < maxRetryCount; i++ {
		ctx1, cancel := context.WithTimeout(ctx, schedulerTimeout)
		err = f(ctx1, c.leaderClient())
		cancel()
		if err == nil {
			return nil
		}

		c.schedulerUpdateLeader()
		select {
		case <-time.After(retryInterval):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New(fmt.Sprintf("failed too many times: %v", err))
}

func (c *client) heartbeatStreamLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		ctx, cancel := context.WithCancel(c.ctx)
		c.connMu.RLock()
		stream, err := c.leaderClient().RegionHeartbeat(ctx)
		c.connMu.RUnlock()
		if err != nil {
			cancel()
			c.schedulerUpdateLeader()
			time.Sleep(retryInterval)
			continue
		}

		errCh := make(chan error, 1)
		wg := &sync.WaitGroup{}
		wg.Add(2)

		go c.reportRegionHeartbeat(ctx, stream, errCh, wg)
		go c.receiveRegionHeartbeat(stream, errCh, wg)
		select {
		case err := <-errCh:
			log.Warnf("[%s][scheduler] heartbeat stream get error: %s ", c.tag, err)
			cancel()
			c.schedulerUpdateLeader()
			time.Sleep(retryInterval)
			wg.Wait()
		case <-c.ctx.Done():
			log.Info("cancel heartbeat stream loop")
			cancel()
			return
		}
	}
}

func (c *client) receiveRegionHeartbeat(stream schedulerpb.Scheduler_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}

		if h := c.heartbeatHandler.Load(); h != nil {
			h.(func(*schedulerpb.RegionHeartbeatResponse))(resp)
		}
	}
}

func (c *client) reportRegionHeartbeat(ctx context.Context, stream schedulerpb.Scheduler_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		request, ok := c.getNextHeartbeatRequest(ctx)
		if !ok {
			return
		}

		request.Header = c.requestHeader()
		err := stream.Send(request)
		if err != nil {
			c.pendingRequest = request
			errCh <- err
			return
		}
	}
}

func (c *client) getNextHeartbeatRequest(ctx context.Context) (*schedulerpb.RegionHeartbeatRequest, bool) {
	if c.pendingRequest != nil {
		req := c.pendingRequest
		c.pendingRequest = nil
		return req, true
	}

	select {
	case <-ctx.Done():
		return nil, false
	case request, ok := <-c.regionCh:
		if !ok {
			return nil, false
		}
		return request, true
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()
	c.connMu.Lock()
	defer c.connMu.Unlock()
	for _, cc := range c.connMu.clientConns {
		cc.Close()
	}
}

func (c *client) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

func (c *client) AllocID(ctx context.Context) (uint64, error) {
	var resp *schedulerpb.AllocIDResponse
	err := c.doRequest(ctx, func(ctx context.Context, client schedulerpb.SchedulerClient) error {
		var err1 error
		resp, err1 = client.AllocID(ctx, &schedulerpb.AllocIDRequest{
			Header: c.requestHeader(),
		})
		return err1
	})
	if err != nil {
		return 0, err
	}
	return resp.GetId(), nil
}

func (c *client) Bootstrap(ctx context.Context, store *metapb.Store) (resp *schedulerpb.BootstrapResponse, err error) {
	err = c.doRequest(ctx, func(ctx context.Context, client schedulerpb.SchedulerClient) error {
		var err1 error
		resp, err1 = client.Bootstrap(ctx, &schedulerpb.BootstrapRequest{
			Header: c.requestHeader(),
			Store:  store,
		})
		return err1
	})
	return resp, err
}

func (c *client) IsBootstrapped(ctx context.Context) (bool, error) {
	var resp *schedulerpb.IsBootstrappedResponse
	err := c.doRequest(ctx, func(ctx context.Context, client schedulerpb.SchedulerClient) error {
		var err1 error
		resp, err1 = client.IsBootstrapped(ctx, &schedulerpb.IsBootstrappedRequest{Header: c.requestHeader()})
		return err1
	})
	if err != nil {
		return false, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return false, errors.New(herr.String())
	}
	return resp.Bootstrapped, nil
}

func (c *client) PutStore(ctx context.Context, store *metapb.Store) error {
	var resp *schedulerpb.PutStoreResponse
	err := c.doRequest(ctx, func(ctx context.Context, client schedulerpb.SchedulerClient) error {
		var err1 error
		resp, err1 = client.PutStore(ctx, &schedulerpb.PutStoreRequest{
			Header: c.requestHeader(),
			Store:  store,
		})
		return err1
	})
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	var resp *schedulerpb.GetStoreResponse
	err := c.doRequest(ctx, func(ctx context.Context, client schedulerpb.SchedulerClient) error {
		var err1 error
		resp, err1 = client.GetStore(ctx, &schedulerpb.GetStoreRequest{
			Header:  c.requestHeader(),
			StoreId: storeID,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Store, nil
}

func (c *client) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	var resp *schedulerpb.GetRegionResponse
	err := c.doRequest(ctx, func(ctx context.Context, client schedulerpb.SchedulerClient) error {
		var err1 error
		resp, err1 = client.GetRegion(ctx, &schedulerpb.GetRegionRequest{
			Header:    c.requestHeader(),
			RegionKey: key,
		})
		return err1
	})
	if err != nil {
		return nil, nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, nil, errors.New(herr.String())
	}
	return resp.Region, resp.Leader, nil
}

func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	var resp *schedulerpb.GetRegionResponse
	err := c.doRequest(ctx, func(ctx context.Context, client schedulerpb.SchedulerClient) error {
		var err1 error
		resp, err1 = client.GetRegionByID(ctx, &schedulerpb.GetRegionByIDRequest{
			Header:   c.requestHeader(),
			RegionId: regionID,
		})
		return err1
	})
	if err != nil {
		return nil, nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, nil, errors.New(herr.String())
	}
	return resp.Region, resp.Leader, nil
}

func (c *client) AskSplit(ctx context.Context, region *metapb.Region) (resp *schedulerpb.AskSplitResponse, err error) {
	err = c.doRequest(ctx, func(ctx context.Context, client schedulerpb.SchedulerClient) error {
		var err1 error
		resp, err1 = client.AskSplit(ctx, &schedulerpb.AskSplitRequest{
			Header: c.requestHeader(),
			Region: region,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp, nil
}

func (c *client) StoreHeartbeat(ctx context.Context, stats *schedulerpb.StoreStats) error {
	var resp *schedulerpb.StoreHeartbeatResponse
	err := c.doRequest(ctx, func(ctx context.Context, client schedulerpb.SchedulerClient) error {
		var err1 error
		resp, err1 = client.StoreHeartbeat(ctx, &schedulerpb.StoreHeartbeatRequest{
			Header: c.requestHeader(),
			Stats:  stats,
		})
		return err1
	})
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) RegionHeartbeat(request *schedulerpb.RegionHeartbeatRequest) error {
	c.regionCh <- request
	return nil
}

func (c *client) SetRegionHeartbeatResponseHandler(_ uint64, h func(*schedulerpb.RegionHeartbeatResponse)) {
	if h == nil {
		h = func(*schedulerpb.RegionHeartbeatResponse) {}
	}
	c.heartbeatHandler.Store(h)
}

func (c *client) requestHeader() *schedulerpb.RequestHeader {
	return &schedulerpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}
