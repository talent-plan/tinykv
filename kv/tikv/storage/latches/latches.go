package latches

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"sync"
)

type Latches struct {
	// Before modifying any property of a key, the thread must have the latch for that key. `Latches` maps each latched
	// key to a WaitGroup. Threads who find a key locked should wait on that WaitGroup.
	latchMap map[string]*sync.WaitGroup
	// Mutex to guard latchMap. A thread must hold this mutex while it makes any change to latchMap.
	latchGuard sync.Mutex
	// An optional validation function, only used for testing.
	Validation func(txn *kvstore.MvccTxn, keys [][]byte)
}

func NewLatches() *Latches {
	l := new(Latches)
	l.latchMap = make(map[string]*sync.WaitGroup)
	return l
}

// acquireLatches locks all Latches needed to execute txn.
func (l *Latches) AcquireLatches(keys [][]byte) *sync.WaitGroup {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	// Check none of the keys we want to write are locked.
	for _, key := range keys {
		if latchWg, ok := l.latchMap[string(key)]; ok {
			// Return a wait group to wait on.
			return latchWg
		}
	}

	// All Latches are available, lock them all with a new wait group.
	wg := new(sync.WaitGroup)
	wg.Add(1)
	for _, key := range keys {
		l.latchMap[string(key)] = wg
	}

	return nil
}

func (l *Latches) ReleaseLatches(keysToLatch [][]byte) {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	first := true
	for _, key := range keysToLatch {
		if first {
			wg := l.latchMap[string(key)]
			wg.Done()
			first = false
		}
		delete(l.latchMap, string(key))
	}
}

func (l *Latches) WaitForLatches(keysToLatch [][]byte) {
	for {
		wg := l.AcquireLatches(keysToLatch)
		if wg == nil {
			return
		}
		wg.Wait()
	}
}

func (l *Latches) Validate(txn *kvstore.MvccTxn, latched [][]byte) {
	if l.Validation != nil {
		l.Validation(txn, latched)
	}
}
