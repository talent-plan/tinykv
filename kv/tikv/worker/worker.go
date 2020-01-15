package worker

import "sync"

type TaskType int64

const (
	TaskTypeStop       TaskType = 0
	TaskTypeRaftLogGC  TaskType = 1
	TaskTypeSplitCheck TaskType = 2

	TaskTypePDAskBatchSplit    TaskType = 102
	TaskTypePDHeartbeat        TaskType = 103
	TaskTypePDStoreHeartbeat   TaskType = 104
	TaskTypePDReportBatchSplit TaskType = 105
	TaskTypePDDestroyPeer      TaskType = 108

	TaskTypeRegionGen   TaskType = 401
	TaskTypeRegionApply TaskType = 402
	/// Destroy data between [start_key, end_key).
	///
	/// The deletion may and may not succeed.
	TaskTypeRegionDestroy TaskType = 403

	TaskTypeResolveAddr TaskType = 501

	TaskTypeSnapSend TaskType = 601
	TaskTypeSnapRecv TaskType = 602
)

type Task struct {
	Tp   TaskType
	Data interface{}
}

type Worker struct {
	name     string
	sender   chan<- Task
	receiver <-chan Task
	closeCh  chan struct{}
	wg       *sync.WaitGroup
}

type TaskHandler interface {
	Handle(t Task)
}

type Starter interface {
	start()
}

func (w *Worker) Start(handler TaskHandler) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if s, ok := handler.(Starter); ok {
			s.start()
		}
		for {
			Task := <-w.receiver
			if Task.Tp == TaskTypeStop {
				return
			}
			handler.Handle(Task)
		}
	}()
}

func (w *Worker) Sender() chan<- Task {
	return w.sender
}

func (w *Worker) Stop() {
	w.sender <- Task{Tp: TaskTypeStop}
}

const defaultWorkerCapacity = 128

func NewWorker(name string, wg *sync.WaitGroup) *Worker {
	ch := make(chan Task, defaultWorkerCapacity)
	return &Worker{
		sender:   (chan<- Task)(ch),
		receiver: (<-chan Task)(ch),
		name:     name,
		wg:       wg,
	}
}
