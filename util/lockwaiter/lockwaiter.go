package lockwaiter

import (
	"sort"
	"sync"
	"time"

	"github.com/ngaut/log"
)

type Manager struct {
	mu            sync.Mutex
	waitingQueues map[uint64]*queue
}

func NewManager() *Manager {
	return &Manager{
		waitingQueues: map[uint64]*queue{},
	}
}

type queue struct {
	mu      sync.Mutex
	waiters []*Waiter
}

func (q *queue) getReadyWaiters(keyHashes []uint64) (readyWaiters []*Waiter, remainSize int) {
	readyWaiters = make([]*Waiter, 0, 8)
	q.mu.Lock()
	remainedWaiters := q.waiters[:0]
	for _, w := range q.waiters {
		if w.inKeys(keyHashes) {
			readyWaiters = append(readyWaiters, w)
		} else {
			remainedWaiters = append(remainedWaiters, w)
		}
	}
	remainSize = len(remainedWaiters)
	q.waiters = remainedWaiters
	q.mu.Unlock()
	return
}

func (q *queue) removeWaiter(w *Waiter) {
	q.mu.Lock()
	for i, waiter := range q.waiters {
		if waiter == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
	q.mu.Unlock()
}

type Waiter struct {
	timeout time.Duration
	ch      chan Result
	startTS uint64
	LockTS  uint64
	KeyHash uint64
}

type Position int

type Result struct {
	Position Position
	CommitTS uint64
}

const WaitTimeout Position = -1

func (w *Waiter) Wait() Result {
	select {
	case <-time.After(w.timeout):
		return Result{Position: WaitTimeout}
	case result := <-w.ch:
		return result
	}
}

func (w *Waiter) inKeys(keyHashes []uint64) bool {
	idx := sort.Search(len(keyHashes), func(i int) bool {
		return keyHashes[i] >= w.KeyHash
	})
	if idx == len(keyHashes) {
		return false
	}
	return keyHashes[idx] == w.KeyHash
}

// Wait waits on a lock until waked by others or timeout.
func (lw *Manager) NewWaiter(startTS, lockTS, keyHash uint64, timeout time.Duration) *Waiter {
	// allocate memory before hold the lock.
	q := new(queue)
	q.waiters = make([]*Waiter, 0, 8)
	waiter := &Waiter{
		timeout: timeout,
		ch:      make(chan Result, 1),
		startTS: startTS,
		LockTS:  lockTS,
		KeyHash: keyHash,
	}
	q.waiters = append(q.waiters, waiter)
	lw.mu.Lock()
	if old, ok := lw.waitingQueues[lockTS]; ok {
		old.waiters = append(old.waiters, waiter)
	} else {
		lw.waitingQueues[lockTS] = q
	}
	lw.mu.Unlock()
	return waiter
}

// WakeUp wakes up waiters that waiting on the transaction.
func (lw *Manager) WakeUp(txn, commitTS uint64, keyHashes []uint64) {
	lw.mu.Lock()
	q := lw.waitingQueues[txn]
	lw.mu.Unlock()
	if q != nil {
		sort.Slice(keyHashes, func(i, j int) bool {
			return keyHashes[i] < keyHashes[j]
		})
		waiters, remainSize := q.getReadyWaiters(keyHashes)
		if remainSize == 0 {
			lw.mu.Lock()
			delete(lw.waitingQueues, txn)
			lw.mu.Unlock()
		}
		sort.Slice(waiters, func(i, j int) bool {
			return waiters[i].startTS < waiters[j].startTS
		})
		for i, w := range waiters {
			w.ch <- Result{Position: Position(i), CommitTS: commitTS}
		}
		log.Info("wakeup", len(waiters), "txns blocked by", txn, keyHashes)
	}
}

// CleanUp removes a waiter from waitingQueues when wait timeout.
func (lw *Manager) CleanUp(w *Waiter) {
	lw.mu.Lock()
	q := lw.waitingQueues[w.LockTS]
	lw.mu.Unlock()
	if q != nil {
		q.removeWaiter(w)
	}
}
