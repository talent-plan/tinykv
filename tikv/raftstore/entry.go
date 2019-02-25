package raftstore

import (
	"go.etcd.io/etcd/raft/raftpb"
	"github.com/pingcap/kvproto/pkg/eraftpb"
)

const (
	entryCtxSynclog      entryCtx = 1
	entryCtxSplit        entryCtx = 1 << 1
	entryCtxPrepareMerge entryCtx = 1 << 2
)

// entryCtx stores flags.
type entryCtx byte

func (pc *entryCtx) SetSyncLog() {
	*pc |= entryCtxSynclog
}

func (pc entryCtx) IsSyncLog() bool {
	return pc&entryCtxSynclog > 0
}

func (pc *entryCtx) SetSplit() {
	*pc |= entryCtxSplit
}

func (pc entryCtx) IsSplit() bool {
	return pc&entryCtxSplit > 0
}

func (pc *entryCtx) SetPrepareMerge() {
	*pc |= entryCtxPrepareMerge
}

func (pc entryCtx) IsPrepareMerge() bool {
	return pc&entryCtxPrepareMerge > 0
}

// The raftpb.Entry is the type used in etcd/raft, but tikv use eraftpb.Entry which have extra field Context.
// We need to implement to etcd/raft interface, so we have to do the conversion.
func convertEEntryToEntry(eEntry *eraftpb.Entry) *raftpb.Entry {
	entry := new(raftpb.Entry)
	entry.Type = raftpb.EntryType(eEntry.EntryType)
	entry.Term = eEntry.Term
	entry.Index = eEntry.Index
	var eCtx entryCtx
	if len(eEntry.Context) > 0 {
		eCtx = entryCtx(eEntry.Context[0])
	}
	entry.Data = append(eEntry.Data, byte(eCtx))
	return entry
}

func convertEntryToEEntry(entry *raftpb.Entry) *eraftpb.Entry {
	eEntry := new(eraftpb.Entry)
	eEntry.EntryType = eraftpb.EntryType(entry.Type)
	eEntry.Term = entry.Term
	eEntry.Index = entry.Index
	lastByteIdx := len(entry.Data) - 1
	eEntry.Context = entry.Data[lastByteIdx:]
	eEntry.Data = entry.Data[:lastByteIdx]
	return eEntry
}
