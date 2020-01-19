package inner_server

import (
	"context"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap/errors"
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

type resolveAddrTask struct {
	storeID  uint64
	callback func(addr string, err error)
}

func newResolverRunner(pdClient pd.Client) *resolverRunner {
	return &resolverRunner{
		pdClient:   pdClient,
		storeAddrs: make(map[uint64]storeAddr),
	}
}

func (r *resolverRunner) Handle(t worker.Task) {
	data := t.Data.(resolveAddrTask)
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
