package raftstore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/coocood/badger/y"
	"github.com/cznic/mathutil"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

type fsm interface {
	isStopped() bool

	/// Set a mailbox to fsm, which should be used to send message to itself.
	setMailbox(mb *mailbox)
	/// Take the mailbox from fsm. Implementation should ensure there will be
	/// no reference to mailbox after calling this method.
	takeMailbox() *mailbox
}

type fsmScheduler interface {
	/// Schedule a fsm for later handles.
	schedule(fsm fsm)
	/// Shutdown the scheduler, which indicates that resouces like
	/// background thread pool should be released.
	shutdown()
}

var _ fsmScheduler = new(normalScheduler)

type normalScheduler struct {
	sender chan<- Msg
}

func (s *normalScheduler) schedule(fsm fsm) {
	s.sender <- NewMsg(MsgTypeFsmNormal, fsm)
}

func (s *normalScheduler) shutdown() {
	close(s.sender)
}

var _ fsmScheduler = new(controlScheduler)

type controlScheduler struct {
	sender chan<- Msg
}

func (s *controlScheduler) schedule(fsm fsm) {
	s.sender <- NewMsg(MsgTypeFsmControl, fsm)
}

func (s *controlScheduler) shutdown() {
	// We don't close the sender here because it will be closed by normalScheduler.
}

const (
	notifyStateNotified = 0
	notifyStateIdle     = 1
	notifyStateDrop     = 2
)

type fsmState struct {
	status int64
	data   unsafe.Pointer
}

/// Every mailbox should have one and only one owner, who will receive all
/// messages sent to this mailbox.
///
/// When a message is sent to a mailbox, its owner will be checked whether it's
/// idle. An idle owner will be scheduled via `fsmScheduler` immediately, which
/// will drive the fsm to poll for messages.
type mailbox struct {
	sender chan<- Msg
	state  fsmState
}

func newMailbox(sender chan<- Msg, fsm fsm) *mailbox {
	mb := &mailbox{sender: sender}
	mb.state.status = notifyStateIdle
	mb.state.data = unsafe.Pointer(&fsm)
	return mb
}

/// Take the owner if it's IDLE.
func (mb *mailbox) takeFsm() fsm {
	swapped := atomic.CompareAndSwapInt64(&mb.state.status, notifyStateIdle, notifyStateNotified)
	if !swapped {
		return nil
	}
	old := atomic.SwapPointer(&mb.state.data, nil)
	if old == nil {
		panic("inconsistent status and data, something should be wrong.")
	}
	return *(*fsm)(old)
}

/// Put the owner back to the state.
///
/// It's not required that all messages should be consumed before
/// releasing a fsm. However, a fsm is guaranteed to be notified only
/// when new messages arrives after it's released.
func (mb *mailbox) release(fsm fsm) {
	prev := atomic.SwapPointer(&mb.state.data, (unsafe.Pointer)(&fsm))
	prevStatus := int64(notifyStateNotified)
	if prev == nil {
		swapped := atomic.CompareAndSwapInt64(&mb.state.status, notifyStateNotified, notifyStateIdle)
		if swapped {
			return
		}
		prevStatus = atomic.LoadInt64(&mb.state.status)
		if prevStatus == notifyStateDrop {
			atomic.SwapPointer(&mb.state.data, nil)
		}
	}
	panic(fmt.Sprintf("invalid release state %d", prevStatus))
}

func (mb *mailbox) len() int {
	return len(mb.sender)
}

func (mb *mailbox) isEmpty() bool {
	return len(mb.sender) == 0
}

func (mb *mailbox) notify(scheduler fsmScheduler) {
	fsm := mb.takeFsm()
	if fsm != nil {
		fsm.setMailbox(mb)
		scheduler.schedule(fsm)
	}
}

func (mb *mailbox) send(msg Msg, scheduler fsmScheduler) {
	mb.sender <- msg
	mb.notify(scheduler)
}

func (mb *mailbox) close() {
	prevState := atomic.SwapInt64(&mb.state.status, notifyStateDrop)
	if prevState == notifyStateDrop || prevState == notifyStateNotified {
		return
	}
	atomic.StorePointer(&mb.state.data, nil)
}

/// router route messages to its target mailbox.
///
/// Every fsm has a mailbox, hence it's necessary to have an address book
/// that can deliver messages to specified fsm, which is exact router.
///
/// In our abstract model, every batch system has two different kind of
/// fsms. First is normal fsm, which does the common work like peers in a
/// raftstore model or apply delegate in apply model. Second is control fsm,
/// which does some work that requires a global view of resources or creates
/// missing fsm for specified address. Normal fsm and control fsm can have
/// different scheduler, but this is not required.
type router struct {
	mu      *sync.RWMutex
	normals map[uint64]*mailbox
	caches  map[uint64]*mailbox

	controlBox *mailbox

	controlScheduler fsmScheduler
	normalScheduler  fsmScheduler
}

func newRouter(controlBox *mailbox, controlScheduler, normalScheduler fsmScheduler) *router {
	return &router{
		mu:               new(sync.RWMutex),
		normals:          make(map[uint64]*mailbox),
		caches:           make(map[uint64]*mailbox),
		controlBox:       controlBox,
		controlScheduler: controlScheduler,
		normalScheduler:  normalScheduler,
	}
}

func (r *router) register(addr uint64, mailbox *mailbox) {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.normals[addr]
	r.normals[addr] = mailbox
	if old != nil {
		old.close()
	}
}

type addrMailboxPair struct {
	addr    uint64
	mailbox *mailbox
}

func (r *router) registerAll(mailboxes []addrMailboxPair) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, mb := range mailboxes {
		old := r.normals[mb.addr]
		r.normals[mb.addr] = mb.mailbox
		if old != nil {
			old.close()
		}
	}
}

func (r *router) mailbox(addr uint64) *mailbox {
	caches := r.caches
	mb, ok := caches[addr]
	if ok {
		if atomic.LoadInt64(&mb.state.status) == notifyStateDrop {
			delete(caches, addr)
			return nil
		}
		return mb
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	mb, ok = r.normals[addr]
	if !ok {
		return nil
	}
	caches[addr] = mb
	return mb
}

var errMailboxNotFound = errors.New("mailbox not found")

func (r *router) send(addr uint64, msg Msg) error {
	mb := r.mailbox(addr)
	if mb == nil {
		return errMailboxNotFound
	}
	mb.send(msg, r.normalScheduler)
	return nil
}

func (r *router) sendControl(msg Msg) {
	r.controlBox.send(msg, r.controlScheduler)
}

func (r *router) broadcastShutdown() {
	log.Info("broadcasting shutdown")
	r.mu.Lock()
	defer r.mu.Unlock()
	for addr, mb := range r.normals {
		log.Debugf("region %d shutdown mailbox", addr)
		mb.close()
		delete(r.normals, addr)
	}
	r.controlBox.close()
	r.controlScheduler.shutdown()
	r.normalScheduler.shutdown()
}

func (r *router) close(addr uint64) {
	log.Infof("region %d shutdown mailbox", addr)
	delete(r.caches, addr)
	r.mu.Lock()
	defer r.mu.Unlock()
	mb, ok := r.normals[addr]
	if ok {
		mb.close()
		delete(r.normals, addr)
	}
}

func (r *router) clone() *router {
	cloned := *r
	cloned.caches = make(map[uint64]*mailbox)
	return &cloned
}

type batch struct {
	normals []fsm
	control fsm
}

func newBatchWithCapacity(cap int) *batch {
	return &batch{
		normals: make([]fsm, 0, cap),
	}
}

func (b *batch) push(msg Msg) {
	switch msg.Type {
	case MsgTypeFsmNormal:
		b.normals = append(b.normals, msg.Data.(fsm))
	case MsgTypeFsmControl:
		y.Assert(b.control == nil)
		b.control = msg.Data.(fsm)
	default:
		panic(fmt.Sprintf("unexpected msg type %d", msg.Type))
	}
}

func (b *batch) len() int {
	l := len(b.normals)
	if b.control != nil {
		l++
	}
	return l
}

func (b *batch) isEmpty() bool {
	return b.len() == 0
}

func (b *batch) clear() {
	b.normals = b.normals[:0]
	b.control = nil
}

/// Put back the FSM located at index.
///
/// Only when channel length is larger than `checked_len` will trigger
/// further notification. This function may fail if channel length is
/// larger than the given value before FSM is released.
func (b *batch) release(index, checkedLen int) bool {
	fsm := b.swapRemove(index)
	mailbox := fsm.takeMailbox()
	mailbox.release(fsm)
	if mailbox.len() == checkedLen {
		return true
	}
	fsm = mailbox.takeFsm()
	if fsm == nil {
		return true
	}
	fsm.setMailbox(mailbox)
	b.swapInsert(index, fsm)
	return false
}

func (b *batch) swapRemove(index int) fsm {
	fsm := b.normals[index]
	lastIdx := len(b.normals) - 1
	b.normals[index] = b.normals[lastIdx]
	b.normals = b.normals[:lastIdx]
	return fsm
}

func (b *batch) swapInsert(index int, fsm fsm) {
	lastIdx := len(b.normals)
	b.normals = append(b.normals, fsm)
	b.normals[index], b.normals[lastIdx] = b.normals[lastIdx], b.normals[index]
}

/// Remove the normal FSM located at `index`.
///
/// This method should only be called when the FSM is stopped.
/// If there are still messages in channel, the FSM is untouched and
/// the function will return false to let caller to keep polling.
func (b *batch) remove(index int) bool {
	fsm := b.swapRemove(index)
	mailbox := fsm.takeMailbox()
	if mailbox.isEmpty() {
		mailbox.release(fsm)
		return true
	}
	fsm.setMailbox(mailbox)
	b.swapInsert(index, fsm)
	return false
}

func (b *batch) releaseControl(controlBox *mailbox, checkedLen int) bool {
	fsm := b.control
	b.control = nil
	controlBox.release(fsm)
	if controlBox.len() == checkedLen {
		return true
	}
	fsm = controlBox.takeFsm()
	if fsm == nil {
		return true
	}
	b.control = fsm
	return false
}

func (b *batch) removeControl(controlBox *mailbox) {
	if controlBox.isEmpty() {
		fsm := b.control
		b.control = nil
		controlBox.release(fsm)
	}
}

/// A handler that poll all FSM in ready.
///
/// A General process works like following:
/// ```text
/// loop {
///     begin
///     if control is ready:
///         handle_control
///     foreach ready normal:
///         handle_normal
///     end
/// }
/// ```
///
/// Note that, every poll thread has its own handler, which doesn't have to be
/// Sync.
type pollHandler interface {

	/// This function is called at the very beginning of every round.
	begin(batchSize int)

	/// This function is called when handling readiness for control FSM.
	///
	/// If the returned value pause is true, the chLen represents the current channel length.
	/// This function will only be called for the same fsm after channel's length is larger than chLen.
	/// If pause is false, then this function will still be called for the same FSM in the next
	/// loop unless the FSM is stopped.
	handleControl(control fsm) (pause bool, chLen int)

	/// This function is called when handling readiness for normal FSM.
	///
	/// The returned value is handled in the same way as `handle_control`.
	handleNormal(normal fsm) (pause bool, chLen int)

	/// This function is called at the end of every round.
	end(normals []fsm)
}

type poller struct {
	router       *router
	fsmReceiver  <-chan Msg
	handler      pollHandler
	maxBatchSize int
}

func (p *poller) fetchBatch(batch *batch, maxSize int) {
	curBatchLen := batch.len()
	if batch.control != nil || curBatchLen >= maxSize {
		// Do nothing if there's a pending control fsm or the batch is already full.
		return
	}
	if curBatchLen == 0 {
		msg, ok := <-p.fsmReceiver
		if !ok {
			return
		}
		batch.push(msg)
		curBatchLen++
	}
	n := mathutil.Min(len(p.fsmReceiver), maxSize-curBatchLen)
	for i := 0; i < n; i++ {
		msg, ok := <-p.fsmReceiver
		if !ok {
			return
		}
		batch.push(msg)
	}
}

func (p *poller) poll() {
	const stoppedLen = -1
	batch := newBatchWithCapacity(p.maxBatchSize)
	exhaustedFsms := make([][2]int, 0, p.maxBatchSize)
	p.fetchBatch(batch, p.maxBatchSize)
	for !batch.isEmpty() {
		exhaustedFsms = exhaustedFsms[:0]
		p.handler.begin(batch.len())
		if batch.control != nil {
			pause, chLen := p.handler.handleControl(batch.control)
			if batch.control.isStopped() {
				batch.removeControl(p.router.controlBox)
			} else if pause {
				batch.releaseControl(p.router.controlBox, chLen)
			}
		}
		for i, fsm := range batch.normals {
			pause, chLen := p.handler.handleNormal(fsm)
			if fsm.isStopped() {
				exhaustedFsms = append(exhaustedFsms, [2]int{i, stoppedLen})
			} else if pause {
				exhaustedFsms = append(exhaustedFsms, [2]int{i, chLen})
			}
		}
		p.handler.end(batch.normals)
		// Because release use `swapRemove` internally, so using pop here
		// to remove the correct FSM.
		for i := len(exhaustedFsms) - 1; i >= 0; i-- {
			tupple := exhaustedFsms[i]
			idx, mark := tupple[0], tupple[1]
			if mark == stoppedLen {
				batch.remove(idx)
			} else {
				batch.release(idx, mark)
			}
		}

		// Fetch batch after every round is finished. It's helpful to protect regions
		// from becoming hungry if some regions are hot points.
		p.fetchBatch(batch, p.maxBatchSize)
	}
}

type pollHandlerBuilder interface {
	build() pollHandler
}

type batchSystem struct {
	namePrefix   string
	router       *router
	receiver     <-chan Msg
	poolSize     int
	maxBatchSize int
	wg           sync.WaitGroup
}

func (bs *batchSystem) start(namePrefix string, builder pollHandlerBuilder) {
	bs.wg.Add(bs.poolSize)
	for i := 0; i < bs.poolSize; i++ {
		handler := builder.build()
		poller := &poller{
			router:       bs.router.clone(),
			fsmReceiver:  bs.receiver,
			handler:      handler,
			maxBatchSize: bs.maxBatchSize,
		}
		go func() {
			poller.poll()
			bs.wg.Done()
		}()
	}
	bs.namePrefix = namePrefix
}

func (bs *batchSystem) shutdown() {
	if bs.namePrefix == "" {
		return
	}
	log.Infof("shutdown batch system %s", bs.namePrefix)
	bs.router.broadcastShutdown()
	bs.wg.Wait()
	log.Infof("batch system %s is stopped", bs.namePrefix)
}

func createBatchSystem(poolSize, maxBatchSize int, sender chan<- Msg, controller fsm) (*router, *batchSystem) {
	controlBox := newMailbox(sender, controller)
	ch := make(chan Msg, 4096)
	normalScheduler := &normalScheduler{sender: (chan<- Msg)(ch)}
	controlScheduler := &controlScheduler{sender: (chan<- Msg)(ch)}
	router := newRouter(controlBox, controlScheduler, normalScheduler)
	return router, &batchSystem{
		router:       router.clone(),
		receiver:     (<-chan Msg)(ch),
		poolSize:     poolSize,
		maxBatchSize: maxBatchSize,
	}
}
