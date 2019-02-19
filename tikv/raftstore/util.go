package raftstore

import (
	"time"
	"sync/atomic"
)

type LeaseState int

const (
	/// The lease is suspicious, may be invalid.
	LeaseState_Suspect LeaseState = 1
	/// The lease is valid.
	LeaseState_Valid
	/// The lease is expired.
	LeaseState_Expired
)

/// Lease records an expired time, for examining the current moment is in lease or not.
/// It's dedicated to the Raft leader lease mechanism, contains either state of
///   1. Suspect Timestamp
///      A suspicious leader lease timestamp, which marks the leader may still hold or lose
///      its lease until the clock time goes over this timestamp.
///   2. Valid Timestamp
///      A valid leader lease timestamp, which marks the leader holds the lease for now.
///      The lease is valid until the clock time goes over this timestamp.
///
/// ```text
/// Time
/// |---------------------------------->
///         ^               ^
///        Now           Suspect TS
/// State:  |    Suspect    |   Suspect
///
/// |---------------------------------->
///         ^               ^
///        Now           Valid TS
/// State:  |     Valid     |   Expired
/// ```
///
/// Note:
///   - Valid timestamp would increase when raft log entries are applied in current term.
///   - Suspect timestamp would be set after the message `MsgTimeoutNow` is sent by current peer.
///     The message `MsgTimeoutNow` starts a leader transfer procedure. During this procedure,
///     current peer as an old leader may still hold its lease or lose it.
///     It's possible there is a new leader elected and current peer as an old leader
///     doesn't step down due to network partition from the new leader. In that case,
///     current peer lose its leader lease.
///     Within this suspect leader lease expire time, read requests could not be performed
///     locally.
///   - The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
///     And the expired timestamp for that leader lease is `commit_ts + lease`,
///     which is `send_ts + max_lease` in short.
type Lease struct {
	// If boundSuspect is not nil, then boundValid must be nil, if boundValid is not nil, then
	// boundSuspect must be nil
	boundSuspect *time.Time
	boundValid *time.Time
	maxLease time.Duration

	maxDrift time.Duration
	lastUpdate time.Time
	remote *RemoteLease

	// Todo: use monotonic_raw instead of time.Now() to fix time jump back issue.
}

func NewLease(maxLease time.Duration) *Lease {
	return &Lease{
		maxLease: maxLease,
		maxDrift: maxLease / 3,
		lastUpdate: time.Time{},
	}
}

/// The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
/// And the expired timestamp for that leader lease is `commit_ts + lease`,
/// which is `send_ts + max_lease` in short.
func (l *Lease) nextExpiredTime(sendTs time.Time) time.Time {
	return sendTs.Add(l.maxLease)
}

/// Renew the lease to the bound.
func (l *Lease) Renew(sendTs time.Time) {
	bound := l.nextExpiredTime(sendTs)
	if l.boundSuspect != nil {
		// Longer than suspect ts
		if l.boundSuspect.Before(bound) {
			l.boundSuspect = nil
			l.boundValid = &bound
		}
	} else if l.boundValid != nil {
		// Longer than valid ts
		if l.boundValid.Before(bound) {
			l.boundValid = &bound
		}
	} else {
		// Or an empty lease
		l.boundValid = &sendTs
	}

	// Renew remote if it's valid.
	if l.boundValid != nil {
		if l.boundValid.Sub(l.lastUpdate) > l.maxDrift {
			l.lastUpdate = *l.boundValid
			if l.remote != nil {
				l.remote.Renew(*l.boundValid)
			}
		}
	}
}

/// Suspect the lease to the bound.
func (l *Lease) Suspect(sendTs time.Time) {
	l.ExpireRemoteLease()
	bound := l.nextExpiredTime(sendTs)
	l.boundValid = nil
	l.boundSuspect = &bound
}

/// Inspect the lease state for the ts or now.
func (l *Lease) Inspect(ts *time.Time) LeaseState {
	if l.boundSuspect != nil {
		return LeaseState_Suspect
	}
	if l.boundValid != nil {
		if ts == nil {
			t := time.Now()
			ts = &t
		}
		if ts.Before(*l.boundValid) {
			return LeaseState_Valid
		} else {
			return LeaseState_Expired
		}
	}
	return LeaseState_Expired
}

func (l *Lease) Expire() {
	l.ExpireRemoteLease()
	l.boundValid = nil
	l.boundSuspect = nil
}

func (l *Lease) ExpireRemoteLease() {
	// Expire remote lease if there is any.
	if l.remote != nil {
		l.remote.Expire()
		l.remote = nil
	}
}

/// Return a new `RemoteLease` if there is none.
func (l *Lease) MaybeNewRemoteLease(term uint64) *RemoteLease {
	if l.remote != nil {
		if l.remote.term == term {
			// At most one connected RemoteLease in the same term.
			return nil
		} else {
			// Term has changed. It is unreachable in the current implementation,
			// because we expire remote lease when leaders step down.
			panic("Must expire the old remote lease first!")
		}
	}
	expiredTime := uint64(0)
	if l.boundValid != nil {
		expiredTime = TimeToU64(*l.boundValid)
	}
	remote := &RemoteLease {
		expiredTime: &expiredTime,
		term: term,
	}
	// Clone the remote.
	remoteClone := &RemoteLease {
		expiredTime: &expiredTime,
		term: term,
	}
	l.remote = remote
	return remoteClone
}

/// A remote lease, it can only be derived by `Lease`. It will be sent
/// to the local read thread, so name it remote. If Lease expires, the remote must
/// expire too.
type RemoteLease struct {
	expiredTime *uint64
	term uint64
}

func (r *RemoteLease) Inspect(ts *time.Time) LeaseState {
	expiredTime := atomic.LoadUint64(r.expiredTime)
	if ts == nil {
		t := time.Now()
		ts = &t
	}
	if ts.Before(U64ToTime(expiredTime)) {
		return LeaseState_Valid
	} else {
		return LeaseState_Expired
	}
}

func (r *RemoteLease) Renew(bound time.Time) {
	atomic.StoreUint64(r.expiredTime, TimeToU64(bound))
}

func (r *RemoteLease) Expire() {
	atomic.StoreUint64(r.expiredTime, 0)
}

func (r *RemoteLease) Term() uint64 {
	return r.term
}

const (
	NSEC_PER_MSEC uint64 = 1000000
	SEC_SHIFT uint64 = 10
	MSEC_MASK uint64 = (1 << SEC_SHIFT) - 1
)

func TimeToU64(t time.Time) uint64 {
	sec := uint64(t.Unix())
	msec := uint64(t.Nanosecond()) / NSEC_PER_MSEC
	sec <<= SEC_SHIFT
	return sec | msec
}

func U64ToTime(u uint64) time.Time {
	sec := u >> SEC_SHIFT
	nsec := (u & MSEC_MASK) * NSEC_PER_MSEC
	return time.Unix(int64(sec), int64(nsec))
}
