package raftstore

import (
	"context"
	"time"

	"github.com/ngaut/unistore/pd"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const storeAddressRefreshSeconds = 60

type storeAddr struct {
	addr       string
	lastUpdate time.Time
}

type resolverRunner struct {
	pdClient   pd.Client
	storeAddrs map[uint64]storeAddr
}

func newResolverRunner(pdClient pd.Client) *resolverRunner {
	return &resolverRunner{
		pdClient:   pdClient,
		storeAddrs: make(map[uint64]storeAddr),
	}
}

func (r *resolverRunner) handle(t task) {
	data := t.data.(resolveAddrTask)
	data.callback(r.getAddr(data.storeID))
}

func (r *resolverRunner) getAddr(id uint64) (string, error) {
	if sa, ok := r.storeAddrs[id]; ok {
		if time.Since(sa.lastUpdate).Seconds() < storeAddressRefreshSeconds {
			return sa.addr, nil
		}
	}
	store, err := r.pdClient.GetStore(context.TODO(), id)
	if err != nil {
		return "", err
	}
	if store.GetState() == metapb.StoreState_Tombstone {
		return "", errors.Errorf("store %d has been removed", id)
	}
	addr := store.GetAddress()
	if addr == "" {
		return "", errors.Errorf("invalid empty address for store %d", id)
	}
	r.storeAddrs[id] = storeAddr{
		addr:       addr,
		lastUpdate: time.Now(),
	}
	return addr, nil
}
