// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, pb.HardState{})
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp *pb.Snapshot) bool {
	if sp == nil || sp.Metadata == nil {
		return true
	}
	return sp.Metadata.Index == 0
}

func mustTerm(term uint64, err error) uint64 {
	if err != nil {
		panic(err)
	}
	return term
}

func nodes(r *Raft) []uint64 {
	nodes := make([]uint64, 0, len(r.Prs))
	for id := range r.Prs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func diffu(a, b string) string {
	if a == b {
		return ""
	}
	aname, bname := mustTemp("base", a), mustTemp("other", b)
	defer os.Remove(aname)
	defer os.Remove(bname)
	cmd := exec.Command("diff", "-u", aname, bname)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// do nothing
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

func mustTemp(pre, body string) string {
	f, err := ioutil.TempFile("", pre)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func ltoa(l *RaftLog) string {
	s := fmt.Sprintf("committed: %d\n", l.committed)
	s += fmt.Sprintf("applied:  %d\n", l.applied)
	for i, e := range l.entries {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func IsLocalMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgHup || msgt == pb.MessageType_MsgBeat
}

func IsResponseMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgAppendResponse || msgt == pb.MessageType_MsgRequestVoteResponse || msgt == pb.MessageType_MsgHeartbeatResponse
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// only for test usage
// to print more information for debug
var enableLog = false
func enableExtraLog() bool {
	return enableLog
}

// From etcd MaybeUpdate
func (prs *Progress) TryUpdate(i uint64) bool {
	if prs.Match < i {
		prs.Match = i
	}

	if prs.Next < i + 1 {
		prs.Next = i + 1
	}
	return true
}

func (r *Raft) maybeCommit() bool {
	maxIdx := r.maxCommittedIdx()
	log.Infof("maybeCommit maxIdx:%d r.term:%d", maxIdx, r.Term)
	return r.RaftLog.tryUpdateCommitted(maxIdx, r.Term)
}

func (r *Raft) maxCommittedIdx() uint64 {
	// majorityconfig is a set of ids that uses majority quorums to make decisions.
	// 从 follower 的 progress 中，得到当前可以被 committed 的 最大 index 值。

	// 利用简单的投票法则
	indexCounter := map[uint64]int{}
	for _, pr := range r.Prs {
		prMatch := pr.Match

		// 如果不存在，则先加入投票池
		if _, ok := indexCounter[prMatch]; !ok {
			indexCounter[prMatch] = 0
		}

		for idx, _ := range indexCounter {
			// 进行投票
			if prMatch >= idx {
				indexCounter[idx] += 1
			}
		}
	}

	maxIdx := uint64(0)
	maxCount := 0
	// find the majority
	for idx, count := range indexCounter {
		if count > maxCount {
			maxIdx = idx
			maxCount = count
		}
	}

	return maxIdx
}

func (rl *RaftLog) tryUpdateCommitted(maxCommittedIdx uint64, term uint64) bool {
	if maxCommittedIdx > rl.committed  {
		maxTerm, err := rl.Term(maxCommittedIdx)
		if err != nil {
			log.WithError(err).Errorf("tryUpdateCommitted with term:%d maxCommittedIdx:%d", term, maxCommittedIdx)
			return false
		}

		if maxTerm != term {
			log.Errorf("tryUpdateCommitted err maxTerm:%d is not equal to term:%d", maxTerm, term)
			return false
		}

		// 这里还有一个判断， term 是否已经过过期的条件没有写完，
		// 暂时还不需要，所以先不写吧 。
		// zeroTermOnErrCompacted(l.term(maxIndx)) == term

		// never decrease commit
		rl.tryCommitToIdx(maxCommittedIdx)
		return true
	}

	return false
}

func (rl *RaftLog) tryCommitToIdx(toCommitIdx uint64) {
	if rl.committed < toCommitIdx {
		if rl.LastIndex() < toCommitIdx {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. " +
				"Was the raft log corrupted, truncated, or lost? ", toCommitIdx, rl.LastIndex())
		}
		rl.committed = toCommitIdx
	}
}