// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"path"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/etcdutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/typeutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pingcap-incubator/tinykv/scheduler/server/member"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// UpdateTimestampStep is used to update timestamp.
	UpdateTimestampStep  = 50 * time.Millisecond
	updateTimestampGuard = time.Millisecond
	maxLogical           = int64(1 << 18)
)

// TimestampOracle is used to maintain the logic of tso.
type TimestampOracle struct {
	// For tso, set after pd becomes leader.
	ts            unsafe.Pointer
	lastSavedTime time.Time
	lease         *member.LeaderLease

	rootPath      string
	member        string
	client        *clientv3.Client
	saveInterval  time.Duration
	maxResetTsGap func() time.Duration
}

// NewTimestampOracle creates a new TimestampOracle.
// TODO: remove saveInterval
func NewTimestampOracle(client *clientv3.Client, rootPath string, member string, saveInterval time.Duration, maxResetTsGap func() time.Duration) *TimestampOracle {
	return &TimestampOracle{
		rootPath:      rootPath,
		client:        client,
		saveInterval:  saveInterval,
		maxResetTsGap: maxResetTsGap,
		member:        member,
	}
}

type atomicObject struct {
	physical time.Time
	logical  int64
}

func (t *TimestampOracle) getTimestampPath() string {
	return path.Join(t.rootPath, "timestamp")
}

func (t *TimestampOracle) loadTimestamp() (time.Time, error) {
	data, err := etcdutil.GetValue(t.client, t.getTimestampPath())
	if err != nil {
		return typeutil.ZeroTime, err
	}
	if len(data) == 0 {
		return typeutil.ZeroTime, nil
	}
	return typeutil.ParseTimestamp(data)
}

// save timestamp, if lastTs is 0, we think the timestamp doesn't exist, so create it,
// otherwise, update it.
func (t *TimestampOracle) saveTimestamp(ts time.Time) error {
	data := typeutil.Uint64ToBytes(uint64(ts.UnixNano()))
	key := t.getTimestampPath()

	leaderPath := path.Join(t.rootPath, "leader")
	txn := kv.NewSlowLogTxn(t.client).If(append([]clientv3.Cmp{}, clientv3.Compare(clientv3.Value(leaderPath), "=", t.member))...)
	resp, err := txn.Then(clientv3.OpPut(key, string(data))).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.New("save timestamp failed, maybe we lost leader")
	}

	t.lastSavedTime = ts

	return nil
}

// SyncTimestamp is used to synchronize the timestamp.
func (t *TimestampOracle) SyncTimestamp(lease *member.LeaderLease) error {
	last, err := t.loadTimestamp()
	if err != nil {
		return err
	}

	next := time.Now()

	// If the current system time minus the saved etcd timestamp is less than `updateTimestampGuard`,
	// the timestamp allocation will start from the saved etcd timestamp temporarily.
	if typeutil.SubTimeByWallClock(next, last) < updateTimestampGuard {
		log.Error("system time may be incorrect", zap.Time("last", last), zap.Time("next", next))
		next = last.Add(updateTimestampGuard)
	}

	save := next.Add(t.saveInterval)
	if err = t.saveTimestamp(save); err != nil {
		return err
	}

	log.Info("sync and save timestamp", zap.Time("last", last), zap.Time("save", save), zap.Time("next", next))

	current := &atomicObject{
		physical: next,
	}
	t.lease = lease
	atomic.StorePointer(&t.ts, unsafe.Pointer(current))

	return nil
}

// ResetUserTimestamp update the physical part with specified tso.
func (t *TimestampOracle) ResetUserTimestamp(tso uint64) error {
	if t.lease == nil || t.lease.IsExpired() {
		return errors.New("Setup timestamp failed, lease expired")
	}
	physical, _ := tsoutil.ParseTS(tso)
	next := physical.Add(time.Millisecond)
	prev := (*atomicObject)(atomic.LoadPointer(&t.ts))

	// do not update
	if typeutil.SubTimeByWallClock(next, prev.physical) <= 3*updateTimestampGuard {
		return errors.New("the specified ts too small than now")
	}

	if typeutil.SubTimeByWallClock(next, prev.physical) >= t.maxResetTsGap() {
		return errors.New("the specified ts too large than now")
	}

	save := next.Add(t.saveInterval)
	if err := t.saveTimestamp(save); err != nil {
		return err
	}
	update := &atomicObject{
		physical: next,
	}
	atomic.CompareAndSwapPointer(&t.ts, unsafe.Pointer(prev), unsafe.Pointer(update))
	return nil
}

// UpdateTimestamp is used to update the timestamp.
// This function will do two things:
// 1. When the logical time is going to be used up, the current physical time needs to increase.
// 2. If the time window is not enough, which means the saved etcd time minus the next physical time
//    is less than or equal to `updateTimestampGuard`, it will need to be updated and save the
//    next physical time plus `TsoSaveInterval` into etcd.
//
// Here is some constraints that this function must satisfy:
// 1. The physical time is monotonically increasing.
// 2. The saved time is monotonically increasing.
// 3. The physical time is always less than the saved timestamp.
func (t *TimestampOracle) UpdateTimestamp() error {
	prev := (*atomicObject)(atomic.LoadPointer(&t.ts))
	now := time.Now()

	jetLag := typeutil.SubTimeByWallClock(now, prev.physical)
	if jetLag > 3*UpdateTimestampStep {
		log.Warn("clock offset", zap.Duration("jet-lag", jetLag), zap.Time("prev-physical", prev.physical), zap.Time("now", now))
	}

	var next time.Time
	prevLogical := atomic.LoadInt64(&prev.logical)
	// If the system time is greater, it will be synchronized with the system time.
	if jetLag > updateTimestampGuard {
		next = now
	} else if prevLogical > maxLogical/2 {
		// The reason choosing maxLogical/2 here is that it's big enough for common cases.
		// Because there is enough timestamp can be allocated before next update.
		log.Warn("the logical time may be not enough", zap.Int64("prev-logical", prevLogical))
		next = prev.physical.Add(time.Millisecond)
	} else {
		// It will still use the previous physical time to alloc the timestamp.
		return nil
	}

	// It is not safe to increase the physical time to `next`.
	// The time window needs to be updated and saved to etcd.
	if typeutil.SubTimeByWallClock(t.lastSavedTime, next) <= updateTimestampGuard {
		save := next.Add(t.saveInterval)
		if err := t.saveTimestamp(save); err != nil {
			return err
		}
	}

	current := &atomicObject{
		physical: next,
		logical:  0,
	}

	atomic.StorePointer(&t.ts, unsafe.Pointer(current))

	return nil
}

// ResetTimestamp is used to reset the timestamp.
func (t *TimestampOracle) ResetTimestamp() {
	zero := &atomicObject{
		physical: typeutil.ZeroTime,
	}
	atomic.StorePointer(&t.ts, unsafe.Pointer(zero))
}

const maxRetryCount = 100

// GetRespTS is used to get a timestamp.
func (t *TimestampOracle) GetRespTS(count uint32) (schedulerpb.Timestamp, error) {
	var resp schedulerpb.Timestamp

	if count == 0 {
		return resp, errors.New("tso count should be positive")
	}

	for i := 0; i < maxRetryCount; i++ {
		current := (*atomicObject)(atomic.LoadPointer(&t.ts))
		if current.physical == typeutil.ZeroTime {
			log.Error("we haven't synced timestamp ok, wait and retry", zap.Int("retry-count", i))
			time.Sleep(200 * time.Millisecond)
			continue
		}

		resp.Physical = current.physical.UnixNano() / int64(time.Millisecond)
		resp.Logical = atomic.AddInt64(&current.logical, int64(count))
		if resp.Logical >= maxLogical {
			log.Error("logical part outside of max logical interval, please check ntp time",
				zap.Reflect("response", resp),
				zap.Int("retry-count", i))
			time.Sleep(UpdateTimestampStep)
			continue
		}
		if t.lease == nil || t.lease.IsExpired() {
			return schedulerpb.Timestamp{}, errors.New("alloc timestamp failed, lease expired")
		}
		return resp, nil
	}
	return resp, errors.New("can not get timestamp")
}
