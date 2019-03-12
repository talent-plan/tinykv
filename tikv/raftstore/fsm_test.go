package raftstore

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

type counter struct {
	count *uint64
	recv  <-chan *Msg
	mb    *mailbox
}

func (c *counter) isStopped() bool {
	return false
}

func (c *counter) setMailbox(mb *mailbox) {
	c.mb = mb
}

func (c *counter) takeMailbox() *mailbox {
	mb := c.mb
	c.mb = nil
	return mb
}

type counterScheduler struct {
	sender chan<- *Msg
}

func (cs *counterScheduler) schedule(fsm fsm) {
	cs.sender <- &Msg{
		Type: MsgTypeFsmNormal,
		Fsm:  fsm,
	}
}

func (sc *counterScheduler) shutdown() {
	close(sc.sender)
}

func pollFsm(fsmReceiver <-chan *Msg, block bool) bool {
	for {
		var msg *Msg
		if block {
			msg = <-fsmReceiver
		} else {
			select {
			case msg = <-fsmReceiver:
			default:
				return false
			}
		}
		if msg == nil {
			return true
		}
		counter := msg.Fsm.(*counter)
		processCounter(counter)
		mb := counter.takeMailbox()
		mb.release(counter)
		counter.setMailbox(mb)
	}
}

func processCounter(counter *counter) {
	for {
		select {
		case m := <-counter.recv:
			// Here we use ComputeDeclinedBytes as int value.
			atomic.AddUint64(counter.count, countFromMsg(m))
		default:
			return
		}
	}
}

func newCounter(cap int) (sender chan<- *Msg, count *uint64, cntr *counter) {
	count = new(uint64)
	ch := make(chan *Msg, cap)
	cntr = &counter{
		count: count,
		recv:  ch,
	}
	return ch, count, cntr
}

// Use ComputeDeclinedBytes as int value.
func countMsg(v uint64) *Msg {
	return &Msg{
		ComputeDeclinedBytes: v,
	}
}

func countFromMsg(msg *Msg) uint64 {
	return msg.ComputeDeclinedBytes
}

func TestRouterBasic(t *testing.T) {
	ctrlSender, ctrlCount, ctrlFsm := newCounter(10)
	normarlSender, normalCount, normalFsm := newCounter(10)
	ctrlBox := newMailbox(ctrlSender, ctrlFsm)

	schedulerCh := make(chan *Msg, msgDefaultChanSize)
	scheduler := &counterScheduler{sender: schedulerCh}

	router := newRouter(ctrlBox, scheduler, scheduler)
	normalBox := newMailbox(normarlSender, normalFsm)
	router.register(2, normalBox)

	// Missing mailbox should report error.
	err := router.send(1, countMsg(1))
	assert.NotNil(t, err)
	assert.True(t, len(schedulerCh) == 0)

	// Existing mailbox should trigger readiness.
	assert.Nil(t, router.send(2, countMsg(1)))
	assert.Nil(t, router.send(2, countMsg(4)))
	assert.Equal(t, 1, len(schedulerCh))
	pollFsm(schedulerCh, false)
	assert.Equal(t, atomic.LoadUint64(normalCount), uint64(5))

	// Control mailbox should also trigger readiness.
	router.sendControl(countMsg(2))
	assert.Equal(t, len(schedulerCh), 1)
	pollFsm(schedulerCh, false)
	assert.Equal(t, atomic.LoadUint64(ctrlCount), uint64(2))
}

type runner struct {
	stopped bool
	recv    <-chan *Msg
	mailbox *mailbox
}

func (r *runner) isStopped() bool {
	return r.stopped
}

func (r *runner) setMailbox(mb *mailbox) {
	r.mailbox = mb
}

func (r *runner) takeMailbox() *mailbox {
	mb := r.mailbox
	r.mailbox = nil
	return mb
}

func newRunner(cap int) (sender chan<- *Msg, r fsm) {
	ch := make(chan *Msg, cap)
	r = &runner{
		recv: ch,
	}
	return ch, r
}

type handleMetrics struct {
	begin   int
	normal  int
	control int
}

type testHandler struct {
	local   *handleMetrics
	router  *router
	metrics *handleMetrics
	mu      *sync.Mutex
}

func (h *testHandler) begin(batchSize int) {
	h.local.begin++
}

func (h *testHandler) handleControl(control fsm) (pause bool, chLen int) {
	h.local.control++
	processRunner(control.(*runner))
	return true, 0
}

func (h *testHandler) handleNormal(normal fsm) (pause bool, chLen int) {
	h.local.normal++
	processRunner(normal.(*runner))
	return true, 0
}

func processRunner(r *runner) {
	for {
		select {
		case m := <-r.recv:
			funcFromMsg(m)()
		default:
			return
		}
	}
}

func (h *testHandler) end(normals []fsm) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.metrics.normal += h.local.normal
	h.metrics.control += h.local.control
	h.metrics.begin += h.local.begin
	h.local = new(handleMetrics)
}

// use Msg.Any to store function.
func funcMsg(f func()) *Msg {
	m := new(Msg)
	m.Any = f
	return m
}

func funcFromMsg(m *Msg) func() {
	return m.Any.(func())
}

type testHandlerBuilder struct {
	router  *router
	metrics *handleMetrics
	mu      *sync.Mutex
}

func (b *testHandlerBuilder) build() pollHandler {
	return &testHandler{
		local:   new(handleMetrics),
		router:  b.router.clone(),
		metrics: b.metrics,
		mu:      b.mu,
	}
}

func (b *testHandlerBuilder) readMetrics() handleMetrics {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := *b.metrics
	return m
}

func TestBatch(t *testing.T) {
	ctrlSender, ctrlFsm := newRunner(10)
	router, system := createBatchSystem(2, 2, ctrlSender, ctrlFsm)
	builder := &testHandlerBuilder{
		router:  router.clone(),
		metrics: new(handleMetrics),
		mu:      new(sync.Mutex),
	}
	system.start("test", builder)
	expectedMetrics := handleMetrics{}
	assert.Equal(t, expectedMetrics, builder.readMetrics())
	ch := make(chan int, 1024)
	r := router.clone()
	router.sendControl(funcMsg(func() {
		sender, runner := newRunner(10)
		mailbox := newMailbox(sender, runner)
		ch <- 1
		r.register(1, mailbox)
	}))
	assert.Equal(t, 1, <-ch)
	err := router.send(1, funcMsg(func() {
		ch <- 2
	}))
	assert.Nil(t, err)
	assert.Equal(t, 2, <-ch)
	system.shutdown()
	expectedMetrics.begin = 2
	expectedMetrics.control = 1
	expectedMetrics.normal = 1
	assert.Equal(t, expectedMetrics, builder.readMetrics())
}
