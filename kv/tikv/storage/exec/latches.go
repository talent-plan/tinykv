package exec

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"sync"
)

type latches struct {
	// Before modifying any property of a key, the thread must have the latch for that key. `latches` maps each latched
	// key to a WaitGroup. Threads who find a key locked should wait on that WaitGroup.
	latchMap map[string]*sync.WaitGroup
	// Mutex to guard latches. A thread must hold this mutex while it makes any change to latches.
	latchGuard sync.Mutex
	// An optional validation function, only used for testing.
	Validate func(txn *kvstore.MvccTxn, keys [][]byte)
}

func newLatches() *latches {
	l := new(latches)
	l.latchMap = make(map[string]*sync.WaitGroup)
	return l
}

// acquireLatches locks all latches needed to execute txn.
func (l *latches) acquireLatches(keys [][]byte) *sync.WaitGroup {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	// Check none of the keys we want to write are locked.
	for _, key := range keys {
		if latchWg, ok := l.latchMap[string(key)]; ok {
			// Return a wait group to wait on.
			return latchWg
		}
	}

	// All latches are available, lock them all with a new wait group.
	wg := new(sync.WaitGroup)
	wg.Add(1)
	for _, key := range keys {
		l.latchMap[string(key)] = wg
	}

	return nil
}

func (l *latches) releaseLatches(keys [][]byte) {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	first := true
	for _, key := range keys {
		if first {
			wg := l.latchMap[string(key)]
			wg.Done()
			first = false
		}
		delete(l.latchMap, string(key))
	}
}

func (l *latches) validate(txn *kvstore.MvccTxn, latched [][]byte) {
	if l.Validate != nil {
		l.Validate(txn, latched)
	}
}
