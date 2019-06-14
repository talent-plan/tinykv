package raftstore

import (
	"fmt"
	"time"

	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rcpb "github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/tidb/util/codec"
)

type raftDBWriter struct {
	router *router
}

func (writer *raftDBWriter) Open() {
	// TODO: stub
}

func (writer *raftDBWriter) Close() {
	// TODO: stub
}

type raftWriteBatch struct {
	ctx      *kvrpcpb.Context
	requests []*rcpb.Request
	startTS  uint64
	commitTS uint64
}

func (wb *raftWriteBatch) Prewrite(key []byte, lock *mvcc.MvccLock) {
	encodedKey := codec.EncodeBytes(nil, key)
	putLock, putDefault, err := mvcc.EncodeLockCFValue(lock)
	if err != nil {
		panic(fmt.Sprintf("Prewrite error when transfering lock. [key: %v, %v]", key, err))
	}
	if len(putDefault) != 0 {
		// Prewrite with large value.
		putDefaultReq := &rcpb.Request{
			CmdType: rcpb.CmdType_Put,
			Put: &rcpb.PutRequest{
				Cf:    "",
				Key:   codec.EncodeUintDesc(encodedKey, lock.StartTS),
				Value: putDefault,
			},
		}
		putLockReq := &rcpb.Request{
			CmdType: rcpb.CmdType_Put,
			Put: &rcpb.PutRequest{
				Cf:    CFLock,
				Key:   encodedKey,
				Value: putLock,
			},
		}
		wb.requests = append(wb.requests, putDefaultReq, putLockReq)
	} else {
		putLockReq := &rcpb.Request{
			CmdType: rcpb.CmdType_Put,
			Put: &rcpb.PutRequest{
				Cf:    CFLock,
				Key:   encodedKey,
				Value: putLock,
			},
		}
		wb.requests = append(wb.requests, putLockReq)
	}
}

func (wb *raftWriteBatch) Commit(key []byte, lock *mvcc.MvccLock) {
	encodedKey := codec.EncodeBytes(nil, key)
	putWriteReq := &rcpb.Request{
		CmdType: rcpb.CmdType_Put,
		Put: &rcpb.PutRequest{
			Cf:    CFWrite,
			Key:   codec.EncodeUintDesc(encodedKey, wb.commitTS),
			Value: mvcc.EncodeWriteCFValue(lock.Op, lock.StartTS, lock.Value),
		},
	}
	delLockReq := &rcpb.Request{
		CmdType: rcpb.CmdType_Delete,
		Delete: &rcpb.DeleteRequest{
			Cf:  CFLock,
			Key: encodedKey,
		},
	}
	wb.requests = append(wb.requests, putWriteReq, delLockReq)
}

func (wb *raftWriteBatch) Rollback(key []byte, deleteLock bool) {
	encodedKey := codec.EncodeBytes(nil, key)
	rollBackReq := &rcpb.Request{
		CmdType: rcpb.CmdType_Put,
		Put: &rcpb.PutRequest{
			Cf:    CFWrite,
			Key:   codec.EncodeUintDesc(encodedKey, wb.startTS),
			Value: mvcc.EncodeWriteCFValue(mvcc.WriteTypeRollback, wb.startTS, nil),
		},
	}
	if deleteLock {
		delLockReq := &rcpb.Request{
			CmdType: rcpb.CmdType_Delete,
			Delete: &rcpb.DeleteRequest{
				Cf:  CFLock,
				Key: encodedKey,
			},
		}
		wb.requests = append(wb.requests, rollBackReq, delLockReq)
	} else {
		wb.requests = append(wb.requests, rollBackReq)
	}
}

func (writer *raftDBWriter) NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
	return &raftWriteBatch{
		ctx:      ctx,
		startTS:  startTS,
		commitTS: commitTS,
	}
}

func (writer *raftDBWriter) Write(batch mvcc.WriteBatch) error {
	b := batch.(*raftWriteBatch)
	ctx := b.ctx
	header := &rcpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
	}
	request := &rcpb.RaftCmdRequest{
		Header:   header,
		Requests: b.requests,
	}
	cmd := &MsgRaftCmd{
		SendTime: time.Now(),
		Request:  request,
		Callback: NewCallback(),
	}
	err := writer.router.sendRaftCommand(cmd)
	if err != nil {
		return err
	}
	cmd.Callback.wg.Wait()
	return writer.checkResponse(cmd.Callback.resp, len(b.requests))
}

type RaftError struct {
	e *errorpb.Error
}

func (re *RaftError) Error() string {
	return re.e.Message
}

func (writer *raftDBWriter) checkResponse(resp *rcpb.RaftCmdResponse, reqCount int) error {
	if resp.Header.Error != nil {
		return &RaftError{e: resp.Header.Error}
	}
	if len(resp.Responses) != reqCount {
		return errors.Errorf("responses count %d is not equal to requests count %d",
			len(resp.Responses), reqCount)
	}
	return nil
}

func (writer *raftDBWriter) DeleteRange(startKey, endKey []byte, latchHandle mvcc.LatchHandle) error {
	return nil // TODO: stub
}

func NewDBWriter(router *RaftstoreRouter) mvcc.DBWriter {
	return &raftDBWriter{router: router.router}
}
