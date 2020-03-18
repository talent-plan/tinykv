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

package member

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/etcdutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

const (
	// The timeout to wait transfer etcd leader to complete.
	moveLeaderTimeout = 5 * time.Second
	requestTimeout    = etcdutil.DefaultRequestTimeout
	slowRequestTime   = etcdutil.DefaultSlowRequestTime
)

// Member is used for the election related logic.
type Member struct {
	leader atomic.Value
	// Etcd and cluster information.
	etcd     *embed.Etcd
	client   *clientv3.Client
	id       uint64              // etcd server id.
	member   *schedulerpb.Member // current PD's info.
	rootPath string
	// memberValue is the serialized string of `member`. It will be save in
	// etcd leader key when the PD node is successfully elected as the leader
	// of the cluster. Every write will use it to check leadership.
	memberValue string
}

// NewMember create a new Member.
func NewMember(etcd *embed.Etcd, client *clientv3.Client, id uint64) *Member {
	return &Member{
		etcd:   etcd,
		client: client,
		id:     id,
	}
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (m *Member) ID() uint64 {
	return m.id
}

// MemberValue returns the member value.
func (m *Member) MemberValue() string {
	return m.memberValue
}

// Member returns the member.
func (m *Member) Member() *schedulerpb.Member {
	return m.member
}

// Etcd returns etcd related information.
func (m *Member) Etcd() *embed.Etcd {
	return m.etcd
}

// IsLeader returns whether the server is leader or not.
func (m *Member) IsLeader() bool {
	// If server is not started. Both leaderID and ID could be 0.
	return m.GetLeaderID() == m.ID()
}

// GetLeaderID returns current leader's member ID.
func (m *Member) GetLeaderID() uint64 {
	return m.GetLeader().GetMemberId()
}

// GetLeader returns current leader of PD cluster.
func (m *Member) GetLeader() *schedulerpb.Member {
	leader := m.leader.Load()
	if leader == nil {
		return nil
	}
	member := leader.(*schedulerpb.Member)
	if member.GetMemberId() == 0 {
		return nil
	}
	return member
}

// EnableLeader sets the member to leader.
func (m *Member) EnableLeader() {
	m.leader.Store(m.member)
}

// DisableLeader reset the leader value.
func (m *Member) DisableLeader() {
	m.leader.Store(&schedulerpb.Member{})
}

// GetLeaderPath returns the path of the leader.
func (m *Member) GetLeaderPath() string {
	return path.Join(m.rootPath, "leader")
}

// CheckLeader checks returns true if it is needed to check later.
func (m *Member) CheckLeader(name string) (*schedulerpb.Member, int64, bool) {
	if m.GetEtcdLeader() == 0 {
		log.Error("no etcd leader, check leader later")
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}

	leader, rev, err := getLeader(m.client, m.GetLeaderPath())
	if err != nil {
		log.Error("get leader meet error", zap.Error(err))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}
	if leader != nil {
		if m.isSameLeader(leader) {
			// oh, we are already leader, we may meet something wrong
			// in previous CampaignLeader. we can delete and campaign again.
			log.Warn("the leader has not changed, delete and campaign again", zap.Stringer("old-leader", leader))
			if err = m.deleteLeaderKey(); err != nil {
				log.Error("delete leader key meet error", zap.Error(err))
				time.Sleep(200 * time.Millisecond)
				return nil, 0, true
			}
		}
	}
	return leader, rev, false
}

// CheckPriority if the leader will be moved according to the priority.
func (m *Member) CheckPriority(ctx context.Context) {
	etcdLeader := m.GetEtcdLeader()
	if etcdLeader == m.ID() || etcdLeader == 0 {
		return
	}
	myPriority, err := m.GetMemberLeaderPriority(m.ID())
	if err != nil {
		log.Error("failed to load leader priority", zap.Error(err))
		return
	}
	leaderPriority, err := m.GetMemberLeaderPriority(etcdLeader)
	if err != nil {
		log.Error("failed to load etcd leader priority", zap.Error(err))
		return
	}
	if myPriority > leaderPriority {
		err := m.MoveEtcdLeader(ctx, etcdLeader, m.ID())
		if err != nil {
			log.Error("failed to transfer etcd leader", zap.Error(err))
		} else {
			log.Info("transfer etcd leader",
				zap.Uint64("from", etcdLeader),
				zap.Uint64("to", m.ID()))
		}
	}
}

// MoveEtcdLeader tries to transfer etcd leader.
func (m *Member) MoveEtcdLeader(ctx context.Context, old, new uint64) error {
	moveCtx, cancel := context.WithTimeout(ctx, moveLeaderTimeout)
	defer cancel()
	return errors.WithStack(m.etcd.Server.MoveLeader(moveCtx, old, new))
}

// getLeader gets server leader from etcd.
func getLeader(c *clientv3.Client, leaderPath string) (*schedulerpb.Member, int64, error) {
	leader := &schedulerpb.Member{}
	ok, rev, err := etcdutil.GetProtoMsgWithModRev(c, leaderPath, leader)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, nil
	}

	return leader, rev, nil
}

// GetEtcdLeader returns the etcd leader ID.
func (m *Member) GetEtcdLeader() uint64 {
	return m.etcd.Server.Lead()
}

func (m *Member) isSameLeader(leader *schedulerpb.Member) bool {
	return leader.GetMemberId() == m.ID()
}

// MemberInfo initializes the member info.
func (m *Member) MemberInfo(cfg *config.Config, name string, rootPath string) {
	leader := &schedulerpb.Member{
		Name:       name,
		MemberId:   m.ID(),
		ClientUrls: strings.Split(cfg.AdvertiseClientUrls, ","),
		PeerUrls:   strings.Split(cfg.AdvertisePeerUrls, ","),
	}

	data, err := leader.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatal("marshal leader meet error", zap.Stringer("leader", leader), zap.Error(err))
	}
	m.member = leader
	m.memberValue = string(data)
	m.rootPath = rootPath
}

// CampaignLeader is used to campaign the leader.
func (m *Member) CampaignLeader(lease *LeaderLease, leaseTimeout int64) error {
	err := lease.Grant(leaseTimeout)
	if err != nil {
		return err
	}

	leaderKey := m.GetLeaderPath()
	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := kv.NewSlowLogTxn(m.client).
		If(clientv3.Compare(clientv3.CreateRevision(leaderKey), "=", 0)).
		Then(clientv3.OpPut(leaderKey, m.memberValue, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.New("failed to campaign leader, other server may campaign ok")
	}
	return nil
}

// ResignLeader resigns current PD's leadership. If nextLeader is empty, all
// other pd-servers can campaign.
func (m *Member) ResignLeader(ctx context.Context, from string, nextLeader string) error {
	log.Info("try to resign leader to next leader", zap.String("from", from), zap.String("to", nextLeader))
	// Determine next leaders.
	var leaderIDs []uint64
	res, err := etcdutil.ListEtcdMembers(m.client)
	if err != nil {
		return err
	}
	for _, member := range res.Members {
		if (nextLeader == "" && member.ID != m.id) || (nextLeader != "" && member.Name == nextLeader) {
			leaderIDs = append(leaderIDs, member.GetID())
		}
	}
	if len(leaderIDs) == 0 {
		return errors.New("no valid pd to transfer leader")
	}
	nextLeaderID := leaderIDs[rand.Intn(len(leaderIDs))]
	return m.MoveEtcdLeader(ctx, m.ID(), nextLeaderID)
}

// LeaderTxn returns txn() with a leader comparison to guarantee that
// the transaction can be executed only if the server is leader.
func (m *Member) LeaderTxn(cs ...clientv3.Cmp) clientv3.Txn {
	txn := kv.NewSlowLogTxn(m.client)
	return txn.If(append(cs, m.leaderCmp())...)
}

func (m *Member) getMemberLeaderPriorityPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/leader_priority", id))
}

// SetMemberLeaderPriority saves a member's priority to be elected as the etcd leader.
func (m *Member) SetMemberLeaderPriority(id uint64, priority int) error {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := m.LeaderTxn().Then(clientv3.OpPut(key, strconv.Itoa(priority))).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("save leader priority failed, maybe not leader")
	}
	return nil
}

// DeleteMemberLeaderPriority removes a member's priority config.
func (m *Member) DeleteMemberLeaderPriority(id uint64) error {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := m.LeaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("delete leader priority failed, maybe not leader")
	}
	return nil
}

// GetMemberLeaderPriority loads a member's priority to be elected as the etcd leader.
func (m *Member) GetMemberLeaderPriority(id uint64) (int, error) {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := etcdutil.EtcdKVGet(m.client, key)
	if err != nil {
		return 0, err
	}
	if len(res.Kvs) == 0 {
		return 0, nil
	}
	priority, err := strconv.ParseInt(string(res.Kvs[0].Value), 10, 32)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return int(priority), nil
}

func (m *Member) deleteLeaderKey() error {
	// delete leader itself and let others start a new election again.
	leaderKey := m.GetLeaderPath()
	resp, err := m.LeaderTxn().Then(clientv3.OpDelete(leaderKey)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (m *Member) leaderCmp() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(m.GetLeaderPath()), "=", m.memberValue)
}

// WatchLeader is used to watch the changes of the leader.
func (m *Member) WatchLeader(serverCtx context.Context, leader *schedulerpb.Member, revision int64) {
	m.leader.Store(leader)
	defer m.leader.Store(&schedulerpb.Member{})

	watcher := clientv3.NewWatcher(m.client)
	defer watcher.Close()

	ctx, cancel := context.WithCancel(serverCtx)
	defer cancel()

	// The revision is the revision of last modification on this key.
	// If the revision is compacted, will meet required revision has been compacted error.
	// In this case, use the compact revision to re-watch the key.
	for {
		rch := watcher.Watch(ctx, m.GetLeaderPath(), clientv3.WithRev(revision))
		for wresp := range rch {
			// meet compacted error, use the compact revision.
			if wresp.CompactRevision != 0 {
				log.Warn("required revision has been compacted, use the compact revision",
					zap.Int64("required-revision", revision),
					zap.Int64("compact-revision", wresp.CompactRevision))
				revision = wresp.CompactRevision
				break
			}
			if wresp.Canceled {
				log.Error("leader watcher is canceled with", zap.Int64("revision", revision), zap.Error(wresp.Err()))
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Info("leader is deleted")
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			// server closed, return
			return
		default:
		}
	}
}

// Close gracefully shuts down all servers/listeners.
func (m *Member) Close() {
	m.Etcd().Close()
}
