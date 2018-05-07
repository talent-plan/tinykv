package tikv

import "sync"

type resolveTask struct {
	regionID  uint64
	regionVer uint64
	txnID     uint64
}

const cacheSize = 1024

type resolveCache struct {
	mu     sync.Mutex
	oldMap map[resolveTask]*sync.WaitGroup
	newMap map[resolveTask]*sync.WaitGroup
}

func (lr *resolveCache) check(task resolveTask) (wg *sync.WaitGroup, resolved bool) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	newWg, ok := lr.newMap[task]
	if ok {
		return newWg, true
	}
	oldWg, ok := lr.oldMap[task]
	if ok {
		return oldWg, true
	}
	wg = new(sync.WaitGroup)
	wg.Add(1)
	lr.newMap[task] = wg
	if len(lr.newMap) == cacheSize {
		lr.oldMap = lr.newMap
		lr.newMap = make(map[resolveTask]*sync.WaitGroup)
	}
	return wg, false
}

func (lr *resolveCache) clear(task resolveTask) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	delete(lr.newMap, task)
	delete(lr.oldMap, task)
}
