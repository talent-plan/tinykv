package raftstore

import "github.com/coocood/badger"

type raftLogGCTask struct {
	raftEngine *badger.DB
	regionID   uint64
	startIdx   uint64
	endIdx     uint64
}
