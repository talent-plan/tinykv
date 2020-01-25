package inner_server

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type MockTransport struct {
	sync.Mutex
	routers map[uint64]message.RaftRouter
}

func NewMockTransport() MockTransport {
	return MockTransport{
		routers: make(map[uint64]message.RaftRouter),
	}
}

func (t *MockTransport) AddNode(storeID uint64, raftRouter message.RaftRouter) {
	t.Lock()
	defer t.Unlock()

	t.routers[storeID] = raftRouter
}

func (t *MockTransport) Send(msg *raft_serverpb.RaftMessage) error {
	t.Lock()
	defer t.Unlock()

	storeID := msg.GetToPeer().GetStoreId()
	router, ok := t.routers[storeID]
	if !ok {
		panic("storeID not found")
	}
	router.SendRaftMessage(msg)

	return nil
}
