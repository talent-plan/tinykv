package raftstore

import "github.com/pingcap/kvproto/pkg/raft_serverpb"

type RaftClient struct {
}

func (c *RaftClient) Send(storeID uint64, addr string, msg *raft_serverpb.RaftMessage) error {
	// TODO
	return nil
}

func (c *RaftClient) GetAddr(storeID uint64) string {
	// TODO
	return ""
}

func (c *RaftClient) InsertAddr(storeID uint64, addr string) {
	// TODO
}

func (c *RaftClient) Flush() {
	// TODO
}
