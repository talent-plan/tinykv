package raftstore

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLease(t *testing.T) {
	sleepTest := func(duration time.Duration, lease *Lease, state LeaseState) {
		time.Sleep(duration)
		end := time.Now()
		assert.Equal(t, lease.Inspect(&end), state)
		assert.Equal(t, lease.Inspect(nil), state)
	}

	duration := 1500 * time.Millisecond

	// Empty lease.
	lease := NewLease(duration)
	remote := lease.MaybeNewRemoteLease(1)
	require.NotNil(t, remote)
	inspectTest := func(lease *Lease, ts *time.Time, state LeaseState) {
		assert.Equal(t, lease.Inspect(ts), state)
		if state == LeaseState_Expired || state == LeaseState_Suspect {
			assert.Equal(t, remote.Inspect(ts), LeaseState_Expired)
		}
	}

	now := time.Now()
	inspectTest(lease, &now, LeaseState_Expired)

	now = time.Now()
	nextExpiredTime := lease.nextExpiredTime(now)
	assert.Equal(t, now.Add(duration), nextExpiredTime)

	// Transit to the Valid state.
	now = time.Now()
	lease.Renew(now)
	inspectTest(lease, &now, LeaseState_Valid)
	inspectTest(lease, nil, LeaseState_Valid)

	// After lease expired time.
	sleepTest(duration, lease, LeaseState_Expired)
	now = time.Now()
	inspectTest(lease, &now, LeaseState_Expired)
	inspectTest(lease, nil, LeaseState_Expired)

	// Transit to the Suspect state.
	now = time.Now()
	lease.Suspect(now)
	inspectTest(lease, &now, LeaseState_Suspect)
	inspectTest(lease, nil, LeaseState_Suspect)

	// After lease expired time, still suspect.
	sleepTest(duration, lease, LeaseState_Suspect)
	now = time.Now()
	inspectTest(lease, &now, LeaseState_Suspect)

	// Clear lease.
	lease.Expire()
	now = time.Now()
	inspectTest(lease, &now, LeaseState_Expired)
	inspectTest(lease, nil, LeaseState_Expired)

	// An expired remote lease can never renew.
	now = time.Now()
	lease.Renew(now.Add(1 * time.Minute))
	assert.Equal(t, remote.Inspect(&now), LeaseState_Expired)

	// A new remote lease.
	m1 := lease.MaybeNewRemoteLease(1)
	require.NotNil(t, m1)
	now = time.Now()
	assert.Equal(t, m1.Inspect(&now), LeaseState_Valid)
}

func TestTimeU64(t *testing.T) {
	type TimeU64 struct {
		T time.Time
		U uint64
	}
	testsTimeToU64 := []TimeU64 {
		TimeU64{ T: time.Unix(0, 0), U: 0 },
		TimeU64{ T: time.Unix(0, 1), U: 0 }, // 1ns will be rounded down to 0ms
		TimeU64{ T: time.Unix(0, 999999), U: 0 }, // 999999ns will be rounded down to 0ms
		TimeU64{ T: time.Unix(1, 0), U: 1 << SEC_SHIFT },
		TimeU64{ T: time.Unix(1, int64(NSEC_PER_MSEC)), U: (1 << SEC_SHIFT) + 1 },
	}

	for _, test := range testsTimeToU64 {
		assert.Equal(t, TimeToU64(test.T), test.U)
	}

	testsU64ToTime := []TimeU64 {
		TimeU64{ T: time.Unix(0, 0), U: 0 },
		TimeU64{ T: time.Unix(0, int64(NSEC_PER_MSEC)), U: 1 },
		TimeU64{ T: time.Unix(1, 0), U: 1 << SEC_SHIFT },
		TimeU64{ T: time.Unix(1, int64(NSEC_PER_MSEC)), U: (1 << SEC_SHIFT) + 1 },
	}
	for _, test := range testsU64ToTime {
		assert.Equal(t, U64ToTime(test.U), test.T)
	}
}
