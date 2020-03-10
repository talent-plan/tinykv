package raft_server

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type raftConn struct {
	streamMu sync.Mutex
	stream   tinykvpb.TinyKv_RaftClient
	ctx      context.Context
	cancel   context.CancelFunc
}

func newRaftConn(addr string, cfg *config.Config) (*raftConn, error) {
	cc, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithInitialWindowSize(2*1024*1024),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                3 * time.Second,
			Timeout:             60 * time.Second,
			PermitWithoutStream: true,
		}))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := tinykvpb.NewTinyKvClient(cc).Raft(ctx)
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
	config *config.Config
	sync.RWMutex
	conn  *raftConn
	addrs map[uint64]string
}

func newRaftClient(config *config.Config) *RaftClient {
	return &RaftClient{
		config: config,
		addrs:  make(map[uint64]string),
	}
}

func (c *RaftClient) getConn(addr string, regionID uint64) (*raftConn, error) {
	c.RLock()
	if c.conn != nil {
		c.RUnlock()
		return c.conn, nil
	}
	c.RUnlock()
	newConn, err := newRaftConn(addr, c.config)
	if err != nil {
		return nil, err
	}
	c.Lock()
	defer c.Unlock()
	if c.conn != nil {
		newConn.Stop()
		return c.conn, nil
	}
	c.conn = newConn
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
	c.conn = nil
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
