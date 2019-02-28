package raftstore

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
