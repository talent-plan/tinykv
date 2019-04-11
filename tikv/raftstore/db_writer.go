package raftstore

import "github.com/ngaut/unistore/tikv/mvcc"

type raftDBWriter struct {
}

func (writer *raftDBWriter) Open() {
	// TODO: stub
}

func (writer *raftDBWriter) Close() {
	// TODO: stub
}

type raftWriteBatch struct {
	startTS  uint64
	commitTS uint64
}

func (wb *raftWriteBatch) Prewrite(key []byte, lock *mvcc.MvccLock) {
	// TODO: stub
}

func (wb *raftWriteBatch) Commit(key []byte, lock *mvcc.MvccLock) {
	// TODO: stub
}

func (wb *raftWriteBatch) Rollback(key []byte, deleteLock bool) {
	// TODO: stub
}

func (writer *raftDBWriter) NewWriteBatch(startTS, commitTS uint64) mvcc.WriteBatch {
	return &raftWriteBatch{
		startTS:  startTS,
		commitTS: commitTS,
	}
}

func (writer *raftDBWriter) Write(batch mvcc.WriteBatch) error {
	return nil // TODO
}

func (writer *raftDBWriter) DeleteRange(startKey, endKey []byte, latchHandle mvcc.LatchHandle) error {
	return nil // TODO: stub
}

func NewDBWriter(bundle *mvcc.DBBundle, config *Config) mvcc.DBWriter {
	// TODO: stub
	return &raftDBWriter{}
}
