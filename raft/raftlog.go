package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *RaftLog) fetchEntries(fromIndex, maxsize uint64) []pb.Entry {
	// TODO: 总决定这里的条件判断有问题。fromIndex = 0 时？
	// if the r.entries is zero ?
	if len(r.entries) == 0 ||
		fromIndex > r.LastIndex() ||
		int(fromIndex) > len(r.entries) - 1 {
		return nil
	}

	// TODO: 这里的算法需要改进
	// (l *raftLog) entries
	return r.entries[fromIndex: min(uint64(len(r.entries) - 1), fromIndex + maxsize)]
}

func copyEntry(ents []pb.Entry) []*pb.Entry {
	ret := make([]*pb.Entry, 0, len(ents))
	for _, e := range ents {
		ret = append(ret, &pb.Entry{
			EntryType: e.EntryType,
			Index: e.Index,
			Term: e.Term,
			Data: e.Data,
		})
	}
	return ret
}

func (r *RaftLog) append(es []*pb.Entry) {
	for _, e := range es {
		r.entries = append(r.entries, *e)
	}
}