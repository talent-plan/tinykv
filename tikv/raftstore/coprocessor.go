package raftstore

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/zhangjinpeng1987/raft"
)

type RegionChangeEvent int

const (
	RegionChangeEvent_Create RegionChangeEvent = 0 + iota
	RegionChangeEvent_Update
	RegionChangeEvent_Destroy
)

type registry struct {
}

type CoprocessorHost struct {
	// Todo: currently it is a place holder
	registry registry
}

func (c *CoprocessorHost) PrePropose(region *metapb.Region, req *raft_cmdpb.RaftCmdRequest) error {
	// Todo: currently it is a place holder
	return nil
}

func (c *CoprocessorHost) OnRegionChanged(region *metapb.Region, event RegionChangeEvent, role raft.StateType) {
	// Todo: currently it is a place holder
}

func (c *CoprocessorHost) OnRoleChanged(region *metapb.Region, role raft.StateType) {
	// Todo: currently it is a place holder
}

func (c *CoprocessorHost) shutdown() {}
