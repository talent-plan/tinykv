package raftstore

import (
	"context"
	"sync"

	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type raftConn struct {
	streamMu sync.Mutex
	stream   tikvpb.Tikv_RaftClient
	ctx      context.Context
	cancel   context.CancelFunc
}

func newRaftConn(addr string, cfg *Config) (*raftConn, error) {
	cc, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithInitialWindowSize(int32(cfg.GrpcInitialWindowSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    cfg.GrpcKeepAliveTime,
			Timeout: cfg.GrpcKeepAliveTimeout,
		}))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := tikvpb.NewTikvClient(cc).Raft(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	return &raftConn{
		stream: stream,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (c *raftConn) Stop() {
	c.cancel()
}

func (c *raftConn) Send(msg *raft_serverpb.RaftMessage) error {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	return c.stream.Send(msg)
}

type connKey struct {
	addr  string
	index int
}

type RaftClient struct {
	config *Config
	sync.RWMutex
	conns map[connKey]*raftConn
	addrs map[uint64]string
}

func newRaftClient(config *Config) *RaftClient {
	return &RaftClient{
		config: config,
		conns:  make(map[connKey]*raftConn),
		addrs:  make(map[uint64]string),
	}
}

func (c *RaftClient) getConn(addr string, regionID uint64) (*raftConn, error) {
	c.RLock()
	key := connKey{addr, int(regionID % c.config.GrpcRaftConnNum)}
	conn, ok := c.conns[key]
	if ok {
		c.RUnlock()
		return conn, nil
	}
	c.RUnlock()
	newConn, err := newRaftConn(addr, c.config)
	if err != nil {
		return nil, err
	}
	c.Lock()
	defer c.Unlock()
	if conn, ok := c.conns[key]; ok {
		newConn.Stop()
		return conn, nil
	}
	c.conns[key] = newConn
	return newConn, nil
}

func (c *RaftClient) Send(storeID uint64, addr string, msg *raft_serverpb.RaftMessage) error {
	conn, err := c.getConn(addr, msg.GetRegionId())
	if err != nil {
		return err
	}
	err = conn.Send(msg)
	if err == nil {
		return nil
	}

	log.Error("raft client failed to send")
	c.Lock()
	defer c.Unlock()
	conn.Stop()
	key := connKey{addr, int(msg.GetRegionId() % c.config.GrpcRaftConnNum)}
	delete(c.conns, key)
	if oldAddr, ok := c.addrs[storeID]; ok && oldAddr == addr {
		delete(c.addrs, storeID)
	}
	return err
}

func (c *RaftClient) GetAddr(storeID uint64) string {
	c.RLock()
	defer c.RUnlock()
	v, _ := c.addrs[storeID]
	return v
}

func (c *RaftClient) InsertAddr(storeID uint64, addr string) {
	c.Lock()
	defer c.Unlock()
	c.addrs[storeID] = addr
}

func (c *RaftClient) Flush() {
	// Not support BufferHint
}
