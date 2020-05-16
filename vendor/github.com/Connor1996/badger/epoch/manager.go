package epoch

import (
	"time"
	"unsafe"
)

type GuardsInspector interface {
	Begin()
	Inspect(payload interface{}, active bool)
	End()
}

type NoOpInspector struct{}

func (i NoOpInspector) Begin()                                   {}
func (i NoOpInspector) Inspect(payload interface{}, active bool) {}
func (i NoOpInspector) End()                                     {}

type Guard struct {
	localEpoch atomicEpoch
	mgr        *ResourceManager
	deletions  []deletion
	payload    interface{}

	next unsafe.Pointer
}

func (g *Guard) Delete(resources []Resource) {
	globalEpoch := g.mgr.currentEpoch.load()
	g.deletions = append(g.deletions, deletion{
		epoch:     globalEpoch,
		resources: resources,
	})
}

func (g *Guard) Done() {
	g.localEpoch.store(g.localEpoch.load().deactivate())
}

func (g *Guard) collect(globalEpoch epoch) bool {
	ds := g.deletions[:0]
	for _, d := range g.deletions {
		if globalEpoch.sub(d.epoch) < 2 {
			ds = append(ds, d)
			continue
		}
		for _, r := range d.resources {
			r.Delete()
		}
		d.resources = nil
	}
	g.deletions = ds
	return len(ds) == 0
}

type Resource interface {
	Delete() error
}

type ResourceManager struct {
	currentEpoch atomicEpoch

	// TODO: cache line size for non x86
	// cachePad make currentEpoch stay in a separate cache line.
	cachePad  [64]byte
	guards    guardList
	inspector GuardsInspector
}

func NewResourceManager(inspector GuardsInspector) *ResourceManager {
	rm := &ResourceManager{
		currentEpoch: atomicEpoch{epoch: 1 << 1},
		inspector:    inspector,
	}
	go rm.collectLoop()
	return rm
}

func (rm *ResourceManager) AcquireWithPayload(payload interface{}) *Guard {
	g := &Guard{
		mgr:     rm,
		payload: payload,
	}
	g.localEpoch.store(rm.currentEpoch.load().activate())
	rm.guards.add(g)
	return g
}

func (rm *ResourceManager) Acquire() *Guard {
	return rm.AcquireWithPayload(nil)
}

func (rm *ResourceManager) collectLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		rm.collect()
	}
}

func (rm *ResourceManager) collect() {
	canAdvance := true
	globalEpoch := rm.currentEpoch.load()

	rm.inspector.Begin()
	rm.guards.iterate(func(guard *Guard) bool {
		localEpoch := guard.localEpoch.load()

		isActive := localEpoch.isActive()
		rm.inspector.Inspect(guard.payload, isActive)

		if isActive {
			canAdvance = canAdvance && localEpoch.sub(globalEpoch) == 0
			return false
		}

		return guard.collect(globalEpoch)
	})
	rm.inspector.End()

	if canAdvance {
		rm.currentEpoch.store(globalEpoch.successor())
	}
}

type deletion struct {
	epoch     epoch
	resources []Resource
}
