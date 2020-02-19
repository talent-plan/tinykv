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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

type Status struct {
	ID uint64

	pb.HardState
	SoftState

	Applied  uint64
	Progress map[uint64]Progress

	LeadTransferee uint64
}

func getProgressCopy(r *Raft) map[uint64]Progress {
	prs := make(map[uint64]Progress)
	for id, p := range r.Prs {
		prs[id] = *p
	}

	return prs
}

func getStatusWithoutProgress(r *Raft) Status {
	s := Status{
		ID:             r.id,
		LeadTransferee: r.leadTransferee,
	}
	s.HardState = r.hardState()
	s.SoftState = *r.softState()
	s.Applied = r.RaftLog.applied
	return s
}

// getStatus gets a copy of the current Raft status.
func getStatus(r *Raft) Status {
	s := getStatusWithoutProgress(r)
	if s.RaftState == StateLeader {
		s.Progress = getProgressCopy(r)
	}
	return s
}

// MarshalJSON translates the Raft status into JSON.
// TODO: try to simplify this by introducing ID type into raft
func (s Status) MarshalJSON() ([]byte, error) {
	j := fmt.Sprintf(`{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%q,"applied":%d,"progress":{`,
		s.ID, s.Term, s.Vote, s.Commit, s.Lead, s.RaftState, s.Applied)

	if len(s.Progress) == 0 {
		j += "},"
	} else {
		for k, v := range s.Progress {
			subj := fmt.Sprintf(`"%x":{"match":%d,"next":%d},`, k, v.Match, v.Next)
			j += subj
		}
		// remove the trailing ","
		j = j[:len(j)-1] + "},"
	}

	j += fmt.Sprintf(`"leadtransferee":"%x"}`, s.LeadTransferee)
	return []byte(j), nil
}

func (s Status) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		raftLogger.Panicf("unexpected error: %v", err)
	}
	return string(b)
}
