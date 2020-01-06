package raftstore

import (
	"encoding/binary"
	"fmt"
)

type applyState struct {
	appliedIndex   uint64
	truncatedIndex uint64
	truncatedTerm  uint64
}

func (s applyState) Marshal() []byte {
	bin := make([]byte, 24)
	binary.LittleEndian.PutUint64(bin, s.appliedIndex)
	binary.LittleEndian.PutUint64(bin[8:], s.truncatedIndex)
	binary.LittleEndian.PutUint64(bin[16:], s.truncatedTerm)
	return bin
}

func (s *applyState) Unmarshal(data []byte) {
	s.appliedIndex = binary.LittleEndian.Uint64(data)
	s.truncatedIndex = binary.LittleEndian.Uint64(data[8:])
	s.truncatedTerm = binary.LittleEndian.Uint64(data[16:])
}

func (s applyState) String() string {
	return fmt.Sprintf("{appliedIndex:%d, truncatedIndex:%d, truncatedTerm:%d}", s.appliedIndex, s.truncatedIndex, s.truncatedTerm)
}

type raftState struct {
	term      uint64
	vote      uint64
	commit    uint64
	lastIndex uint64
}

func (s raftState) Marshal() []byte {
	bin := make([]byte, 32)
	binary.LittleEndian.PutUint64(bin, s.term)
	binary.LittleEndian.PutUint64(bin[8:], s.vote)
	binary.LittleEndian.PutUint64(bin[16:], s.commit)
	binary.LittleEndian.PutUint64(bin[24:], s.lastIndex)
	return bin
}

func (s raftState) String() string {
	return fmt.Sprintf("{term:%d, vote:%d, commit:%d, lastIndex:%d}", s.term, s.vote, s.commit, s.lastIndex)
}

func (s *raftState) Unmarshal(data []byte) {
	s.term = binary.LittleEndian.Uint64(data)
	s.vote = binary.LittleEndian.Uint64(data[8:])
	s.commit = binary.LittleEndian.Uint64(data[16:])
	s.lastIndex = binary.LittleEndian.Uint64(data[24:])
}
