package raftstore

type snapRunner struct {
	snapManager   *SnapManager
	sending_count uint64
	recving_count uint64
}

func newSnapRunner(snapManager *SnapManager) *snapRunner {
	return &snapRunner{
		snapManager: snapManager,
	}
}

func (r *snapRunner) run(t task) {
	switch t.tp {
	case taskTypeSnapSend:
		r.send(t.data.(sendSnapTask))
	case taskTypeSnapRecv:
		r.recv(t.data.(recvSnapTask))
	}
}

func (r *snapRunner) send(t sendSnapTask) {
	// TODO
}

func (r *snapRunner) recv(t recvSnapTask) {
	// TODO
}
