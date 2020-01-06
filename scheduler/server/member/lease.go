// Copyright 2019 PingCAP, Inc.
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

package member

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// LeaderLease is used for renewing leadership of PD server.
type LeaderLease struct {
	client       *clientv3.Client
	lease        clientv3.Lease
	ID           clientv3.LeaseID
	leaseTimeout time.Duration

	expireTime atomic.Value
}

// NewLeaderLease creates a lease.
func NewLeaderLease(client *clientv3.Client) *LeaderLease {
	return &LeaderLease{
		client: client,
		lease:  clientv3.NewLease(client),
	}
}

// Grant uses `lease.Grant` to initialize the lease and expireTime.
func (l *LeaderLease) Grant(leaseTimeout int64) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(l.client.Ctx(), requestTimeout)
	leaseResp, err := l.lease.Grant(ctx, leaseTimeout)
	cancel()
	if err != nil {
		return errors.WithStack(err)
	}
	if cost := time.Since(start); cost > slowRequestTime {
		log.Warn("lease grants too slow", zap.Duration("cost", cost))
	}
	l.ID = leaseResp.ID
	l.leaseTimeout = time.Duration(leaseTimeout) * time.Second
	l.expireTime.Store(start.Add(time.Duration(leaseResp.TTL) * time.Second))
	return nil
}

const revokeLeaseTimeout = time.Second

// Close releases the lease.
func (l *LeaderLease) Close() error {
	// Reset expire time.
	l.expireTime.Store(time.Time{})
	// Try to revoke lease to make subsequent elections faster.
	ctx, cancel := context.WithTimeout(l.client.Ctx(), revokeLeaseTimeout)
	defer cancel()
	l.lease.Revoke(ctx, l.ID)
	return l.lease.Close()
}

// IsExpired checks if the lease is expired. If it returns true, current PD
// server should step down and try to re-elect again.
func (l *LeaderLease) IsExpired() bool {
	return time.Now().After(l.expireTime.Load().(time.Time))
}

// KeepAlive auto renews the lease and update expireTime.
func (l *LeaderLease) KeepAlive(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timeCh := l.keepAliveWorker(ctx, l.leaseTimeout/3)

	var maxExpire time.Time
	for {
		select {
		case t := <-timeCh:
			if t.After(maxExpire) {
				maxExpire = t
				l.expireTime.Store(t)
			}
		case <-time.After(l.leaseTimeout):
			return
		case <-ctx.Done():
			return
		}
	}
}

// Periodically call `lease.KeepAliveOnce` and post back latest received expire time into the channel.
func (l *LeaderLease) keepAliveWorker(ctx context.Context, interval time.Duration) <-chan time.Time {
	ch := make(chan time.Time)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			go func() {
				start := time.Now()
				ctx1, cancel := context.WithTimeout(ctx, time.Duration(l.leaseTimeout))
				defer cancel()
				res, err := l.lease.KeepAliveOnce(ctx1, l.ID)
				if err != nil {
					log.Warn("leader lease keep alive failed", zap.Error(err))
					return
				}
				if res.TTL > 0 {
					expire := start.Add(time.Duration(res.TTL) * time.Second)
					select {
					case ch <- expire:
					case <-ctx1.Done():
					}
				}
			}()

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return ch
}
