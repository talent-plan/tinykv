package tikv

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/coocood/badger"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/ngaut/unistore/util/lockwaiter"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

var _ = Suite(&testMvccSuite{})
var maxTs = uint64(math.MaxUint64)
var lockTTL = uint64(50)
var pessimisitcLockTTL = uint64(5000)

type testMvccSuite struct{}

type TestStore struct {
	MvccStore *MVCCStore
	Svr       *Server
	DBPath    string
	LogPath   string
	c         *C
}

func (ts *TestStore) newReqCtx() *requestCtx {
	return ts.newReqCtxWithKeys([]byte{'t'}, []byte{'u'})
}

func (ts *TestStore) newReqCtxWithKeys(startKey, endKey []byte) *requestCtx {
	return &requestCtx{
		regCtx: &regionCtx{
			latches:  make(map[uint64]*sync.WaitGroup),
			startKey: startKey,
			endKey:   endKey,
		},
		svr: ts.Svr,
	}
}

func newMutation(op kvrpcpb.Op, key, value []byte) *kvrpcpb.Mutation {
	return &kvrpcpb.Mutation{
		Op:    op,
		Key:   key,
		Value: value,
	}
}

func CreateTestDB(dbPath, LogPath string) (*badger.DB, error) {
	subPath := fmt.Sprintf("/%d", 0)
	opts := badger.DefaultOptions
	opts.Dir = dbPath + subPath
	opts.ValueDir = LogPath + subPath
	return badger.Open(opts)
}

func NewTestStore(dbPrefix string, logPrefix string, c *C) (*TestStore, error) {
	dbPath, err := ioutil.TempDir("", dbPrefix)
	if err != nil {
		return nil, err
	}
	LogPath, err := ioutil.TempDir("", logPrefix)
	if err != nil {
		return nil, err
	}
	safePoint := &SafePoint{}
	db, err := CreateTestDB(dbPath, LogPath)
	if err != nil {
		return nil, err
	}
	dbBundle := &mvcc.DBBundle{
		DB:            db,
		LockStore:     lockstore.NewMemStore(4096),
		RollbackStore: lockstore.NewMemStore(4096),
	}
	// Some raft store path problems could not be found using simple store in tests
	// writer := NewDBWriter(dbBundle, safePoint)
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")
	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)
	engines := raftstore.NewEngines(dbBundle, dbBundle.DB, kvPath, raftPath)
	writer := raftstore.NewTestRaftWriter(dbBundle, engines)

	store := NewMVCCStore(dbBundle, dbPath, safePoint, writer, nil)
	svr := NewServer(nil, store, nil)
	return &TestStore{
		MvccStore: store,
		Svr:       svr,
		DBPath:    dbPath,
		LogPath:   LogPath,
		c:         c,
	}, nil
}

func CleanTestStore(store *TestStore) {
	os.RemoveAll(store.DBPath)
	os.RemoveAll(store.LogPath)
}

// PessimisticLock will add pessimistic lock on key
func PessimisticLock(pk []byte, key []byte, startTs uint64, lockTTL uint64, forUpdateTs uint64,
	isFirstLock bool, store *TestStore) (*lockwaiter.Waiter, error) {
	req := &kvrpcpb.PessimisticLockRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_PessimisticLock, key, nil)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
		ForUpdateTs:  forUpdateTs,
		IsFirstLock:  isFirstLock,
	}
	waiter, err := store.MvccStore.PessimisticLock(store.newReqCtx(), req)
	return waiter, err
}

// PrewriteOptimistic raises optimistic prewrite requests on store
func PrewriteOptimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	minCommitTs uint64, store *TestStore) error {
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Put, key, value)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
		MinCommitTs:  minCommitTs,
	}
	return store.MvccStore.prewriteOptimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
}

// PrewritePessimistic raises pessmistic prewrite requests
func PrewritePessimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	isPessimisticLock []bool, forUpdateTs uint64, store *TestStore) error {
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:         []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Put, key, value)},
		PrimaryLock:       pk,
		StartVersion:      startTs,
		LockTtl:           lockTTL,
		IsPessimisticLock: isPessimisticLock,
		ForUpdateTs:       forUpdateTs,
	}
	return store.MvccStore.prewritePessimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
}

func CheckTxnStatus(pk []byte, lockTs uint64, callerStartTs uint64,
	currentTs uint64, rollbackIfNotExists bool, store *TestStore) (uint64, uint64, kvrpcpb.Action, error) {
	req := &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         pk,
		LockTs:             lockTs,
		CallerStartTs:      callerStartTs,
		CurrentTs:          currentTs,
		RollbackIfNotExist: rollbackIfNotExists,
	}
	resTTL, resCommitTs, action, err := store.MvccStore.CheckTxnStatus(store.newReqCtx(), req)
	return resTTL, resCommitTs, action, err
}

func MustLocked(key []byte, pessimistic bool, store *TestStore) {
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(lock, NotNil)
	if pessimistic {
		store.c.Assert(lock.ForUpdateTS, Greater, uint64(0))
		store.c.Assert(lock.Op, Equals, kvrpcpb.Op_PessimisticLock)
	} else {
		store.c.Assert(lock.ForUpdateTS, Equals, uint64(0))
	}
}

func MustUnLocked(key []byte, store *TestStore) {
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(lock, IsNil)
}

func MustPrewritePut(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	MustPrewriteOptimistic(pk, key, val, startTs, 50, startTs, store)
}

func MustPrewriteLockErr(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	err := PrewriteOptimistic(pk, key, val, startTs, lockTTL, startTs, store)
	store.c.Assert(err, NotNil)
	lockedErr := err.(*ErrLocked)
	store.c.Assert(lockedErr, NotNil)
}

func MustPrewritePutErr(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	err := PrewriteOptimistic(pk, key, val, startTs, lockTTL, startTs, store)
	store.c.Assert(err, NotNil)
}

func MustPrewriteDelete(pk, key []byte, startTs uint64, store *TestStore) {
	MustPrewriteOptimistic(pk, key, nil, startTs, 50, startTs, store)
}

func MustAcquirePessimisticLock(pk, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	_, err := PessimisticLock(pk, key, startTs, 500, forUpdateTs, false, store)
	store.c.Assert(err, IsNil)
}

func MustPessimisitcPrewriteDelete(pk, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	MustPrewritePessimistic(pk, key, nil, startTs, 5000, []bool{true}, forUpdateTs, store)
}

func MustPrewriteOptimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	minCommitTs uint64, store *TestStore) {
	store.c.Assert(PrewriteOptimistic(pk, key, value, startTs, lockTTL, minCommitTs, store), IsNil)
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(uint64(lock.TTL), Equals, lockTTL)
	store.c.Assert(bytes.Compare(lock.Value, value), Equals, 0)
}

func MustPrewritePessimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	isPessimisticLock []bool, forUpdateTs uint64, store *TestStore) {
	store.c.Assert(PrewritePessimistic(pk, key, value, startTs, lockTTL, isPessimisticLock, forUpdateTs, store), IsNil)
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(lock.ForUpdateTS, Equals, forUpdateTs)
	store.c.Assert(bytes.Compare(lock.Value, value), Equals, 0)
}

func MustCommitKeyPut(key, val []byte, startTs, commitTs uint64, store *TestStore) {
	err := store.MvccStore.Commit(store.newReqCtx(), [][]byte{key}, startTs, commitTs)
	store.c.Assert(err, IsNil)
	getVal, err := store.newReqCtx().getDBReader().Get(key, commitTs)
	store.c.Assert(err, IsNil)
	store.c.Assert(bytes.Compare(getVal, val), Equals, 0)
}

func MustCommit(key []byte, startTs, commitTs uint64, store *TestStore) {
	err := store.MvccStore.Commit(store.newReqCtx(), [][]byte{key}, startTs, commitTs)
	store.c.Assert(err, IsNil)
}

func MustRollbackKey(key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Rollback(store.newReqCtx(), [][]byte{key}, startTs)
	store.c.Assert(err, IsNil)
	rollbackKey := mvcc.EncodeRollbackKey(nil, key, startTs)
	res := store.MvccStore.rollbackStore.Get(rollbackKey, nil)
	store.c.Assert(bytes.Compare(res, []byte{0}), Equals, 0)
}

func MustGetNone(key []byte, startTs uint64, store *TestStore) {
	val := MustGet(key, startTs, store)
	store.c.Assert(len(val), Equals, 0)
}

func MustGetVal(key, val []byte, startTs uint64, store *TestStore) {
	getVal := MustGet(key, startTs, store)
	store.c.Assert(val, DeepEquals, getVal)
}

func MustGetErr(key []byte, startTs uint64, store *TestStore) {
	_, err := kvGet(key, startTs, store)
	store.c.Assert(err, NotNil)
}

func kvGet(key []byte, readTs uint64, store *TestStore) ([]byte, error) {
	err := store.MvccStore.CheckKeysLock(readTs, key)
	if err != nil {
		return nil, err
	}
	getVal, err := store.newReqCtx().getDBReader().Get(key, readTs)
	return getVal, err
}

func MustGet(key []byte, readTs uint64, store *TestStore) (val []byte) {
	val, err := kvGet(key, readTs, store)
	store.c.Assert(err, IsNil)
	return val
}

func MustPrewriteLock(pk []byte, key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Prewrite(store.newReqCtx(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Lock, key, nil)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
	})
	store.c.Assert(err, IsNil)
}

func (s *testMvccSuite) TestBasicOptimistic(c *C) {
	var err error
	store, err := NewTestStore("basic_optimistic_db", "basic_optimistic_log", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	key1 := []byte("key1")
	val1 := []byte("val1")
	ttl := uint64(200)
	MustPrewriteOptimistic(key1, key1, val1, 1, ttl, 0, store)
	MustCommitKeyPut(key1, val1, 1, 2, store)
	// Read using smaller ts results in nothing
	getVal, err := store.newReqCtx().getDBReader().Get(key1, 1)
	c.Assert(getVal, IsNil)
}

func (s *testMvccSuite) TestPessimiticTxnTTL(c *C) {
	var err error
	store, err := NewTestStore("pessimisitc_txn_ttl_db", "pessimisitc_txn_ttl_log", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	// Pessimisitc lock key1
	key1 := []byte("key1")
	val1 := []byte("val1")
	startTs := uint64(1)
	lockTTL := uint64(1000)
	_, err = PessimisticLock(key1, key1, startTs, lockTTL, startTs, true, store)
	c.Assert(err, IsNil)

	// Prewrite key1 with smaller lock ttl, lock ttl will not be changed
	MustPrewritePessimistic(key1, key1, val1, startTs, lockTTL-500, []bool{true}, startTs, store)
	lock := store.MvccStore.getLock(store.newReqCtx(), key1)
	c.Assert(uint64(lock.TTL), Equals, uint64(1000))

	key2 := []byte("key2")
	val2 := []byte("val2")
	_, err = PessimisticLock(key2, key2, 3, 300, 3, true, store)
	c.Assert(err, IsNil)

	// Prewrite key1 with larger lock ttl, lock ttl will be updated
	MustPrewritePessimistic(key2, key2, val2, 3, 2000, []bool{true}, 3, store)
	lock2 := store.MvccStore.getLock(store.newReqCtx(), key2)
	c.Assert(uint64(lock2.TTL), Equals, uint64(2000))
}

func (s *testMvccSuite) TestRollback(c *C) {
	var err error
	store, err := NewTestStore("RollbackData", "RollbackLog", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	key := []byte("key")
	val := []byte("value")
	startTs := uint64(1)
	lockTTL := uint64(100)
	// Add a Rollback whose start ts is 1.
	MustPrewriteOptimistic(key, key, val, startTs, lockTTL, 0, store)
	MustRollbackKey(key, startTs, store)

	MustPrewriteOptimistic(key, key, val, startTs+1, lockTTL, 0, store)
	MustRollbackKey(key, startTs+1, store)
	var buf []byte
	// Rollback entry still exits in rollbackStore if no rollbackGC
	rollbackKey := mvcc.EncodeRollbackKey(buf, key, startTs)
	res := store.MvccStore.rollbackStore.Get(rollbackKey, nil)
	c.Assert(bytes.Compare(res, []byte{0}), Equals, 0)
}

func (s *testMvccSuite) TestOverwritePessimisitcLock(c *C) {
	var err error
	store, err := NewTestStore("OverWritePessimisticData", "OverWritePessimisticLog", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	key := []byte("key")
	startTs := uint64(1)
	lockTTL := uint64(100)
	forUpdateTs := uint64(100)
	// pessimistic lock one key
	_, err = PessimisticLock(key, key, startTs, lockTTL, forUpdateTs, true, store)
	c.Assert(err, IsNil)
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	c.Assert(lock.ForUpdateTS, Equals, forUpdateTs)

	// pessimistic lock this key again using larger forUpdateTs
	_, err = PessimisticLock(key, key, startTs, lockTTL, forUpdateTs+7, true, store)
	c.Assert(err, IsNil)
	lock2 := store.MvccStore.getLock(store.newReqCtx(), key)
	c.Assert(lock2.ForUpdateTS, Equals, forUpdateTs+7)

	// pessimistic lock one key using smaller forUpdateTsTs
	_, err = PessimisticLock(key, key, startTs, lockTTL, forUpdateTs-7, true, store)
	c.Assert(err, IsNil)
	lock3 := store.MvccStore.getLock(store.newReqCtx(), key)
	c.Assert(lock3.ForUpdateTS, Equals, forUpdateTs+7)
}

func (s *testMvccSuite) TestCheckTxnStatus(c *C) {
	var err error
	store, err := NewTestStore("CheckTxnStatusDB", "CheckTxnStatusLog", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	var resTTL, resCommitTs uint64
	var action kvrpcpb.Action
	pk := []byte("tpk")
	startTs := uint64(1)
	callerStartTs := uint64(3)
	currentTs := uint64(5)

	// Try to check a not exist thing.
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, callerStartTs, currentTs, true, store)
	c.Assert(resTTL, Equals, uint64(0))
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(action, Equals, kvrpcpb.Action_LockNotExistRollback)
	c.Assert(err, IsNil)

	// Using same startTs, prewrite will fail, since checkTxnStatus has rollbacked the key
	val := []byte("val")
	lockTTL := uint64(100)
	minCommitTs := uint64(20)
	err = PrewriteOptimistic(pk, pk, val, startTs, lockTTL, minCommitTs, store)
	c.Assert(err, Equals, ErrAlreadyRollback)

	// Prewrite a large txn
	startTs = 2
	MustPrewriteOptimistic(pk, pk, val, startTs, lockTTL, minCommitTs, store)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, callerStartTs, currentTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)

	// Update min_commit_ts to current_ts. minCommitTs 20 -> 25
	newCallerTs := uint64(25)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, newCallerTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)
	lock := store.MvccStore.getLock(store.newReqCtx(), pk)
	c.Assert(lock.StartTS, Equals, startTs)
	c.Assert(uint64(lock.TTL), Equals, lockTTL)
	c.Assert(lock.MinCommitTS, Equals, newCallerTs+1)

	// When caller_start_ts < lock.min_commit_ts, here 25 < 26, no need to update it.
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, newCallerTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)
	lock = store.MvccStore.getLock(store.newReqCtx(), pk)
	c.Assert(lock.StartTS, Equals, startTs)
	c.Assert(uint64(lock.TTL), Equals, lockTTL)
	c.Assert(lock.MinCommitTS, Equals, newCallerTs+1)

	// current_ts(25) < lock.min_commit_ts(26) < caller_start_ts(35)
	currentTs = uint64(25)
	newCallerTs = 35
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, currentTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)
	lock = store.MvccStore.getLock(store.newReqCtx(), pk)
	c.Assert(lock.StartTS, Equals, startTs)
	c.Assert(uint64(lock.TTL), Equals, lockTTL)
	c.Assert(lock.MinCommitTS, Equals, newCallerTs+1) // minCommitTS updated to 36

	// current_ts is max value 40, but no effect since caller_start_ts is smaller than minCommitTs
	currentTs = uint64(40)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, currentTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)
	lock = store.MvccStore.getLock(store.newReqCtx(), pk)
	c.Assert(lock.StartTS, Equals, startTs)
	c.Assert(uint64(lock.TTL), Equals, lockTTL)
	c.Assert(lock.MinCommitTS, Equals, newCallerTs+1) // minCommitTS updated to 36

	// commit this key, commitTs(35) smaller than minCommitTs(36)
	commitTs := uint64(35)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk}, startTs, commitTs)
	c.Assert(err, NotNil)

	// commit this key, using correct commitTs
	commitTs = uint64(41)
	MustCommitKeyPut(pk, val, startTs, commitTs, store)

	// check committed txn status
	currentTs = uint64(42)
	newCallerTs = uint64(42)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, currentTs, true, store)
	c.Assert(resTTL, Equals, uint64(0))
	c.Assert(resCommitTs, Equals, uint64(41))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_NoAction)
}

func (s *testMvccSuite) TestDecodeOldKey(c *C) {
	rawKey := []byte("trawKey")
	oldCommitTs := uint64(1)
	oldKey := mvcc.EncodeOldKey(rawKey, oldCommitTs)

	resKey, resTs, err := mvcc.DecodeOldKey(oldKey)
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(resKey, rawKey), Equals, 0)
	c.Assert(resTs, Equals, oldCommitTs)
}

func (s *testMvccSuite) TestMvccGet(c *C) {
	var err error
	store, err := NewTestStore("TestMvccGetBy", "TestMvccGetBy", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	lockTTL := uint64(100)
	pk := []byte("t1_r1")
	pkVal := []byte("pkVal")
	startTs1 := uint64(1)
	commitTs1 := uint64(2)
	// write one record
	MustPrewriteOptimistic(pk, pk, pkVal, startTs1, lockTTL, 0, store)
	MustCommitKeyPut(pk, pkVal, startTs1, commitTs1, store)

	// update this record
	startTs2 := uint64(3)
	commitTs2 := uint64(4)
	newVal := []byte("aba")
	MustPrewriteOptimistic(pk, pk, newVal, startTs2, lockTTL, 0, store)
	MustCommitKeyPut(pk, newVal, startTs2, commitTs2, store)

	// read using mvcc
	var res *kvrpcpb.MvccInfo
	res, err = store.MvccStore.MvccGetByKey(store.newReqCtx(), pk)
	c.Assert(err, IsNil)
	c.Assert(len(res.Writes), Equals, 2)

	// prewrite and then rollback
	// Add a Rollback whose start ts is 5.
	startTs3 := uint64(5)
	rollbackVal := []byte("rollbackVal")
	MustPrewriteOptimistic(pk, pk, rollbackVal, startTs3, lockTTL, 0, store)
	MustRollbackKey(pk, startTs3, store)

	// put empty value
	startTs4 := uint64(7)
	commitTs4 := uint64(8)
	emptyVal := []byte("")
	MustPrewriteOptimistic(pk, pk, emptyVal, startTs4, lockTTL, 0, store)
	MustCommitKeyPut(pk, emptyVal, startTs4, commitTs4, store)

	// read using mvcc
	res, err = store.MvccStore.MvccGetByKey(store.newReqCtx(), pk)
	c.Assert(err, IsNil)
	c.Assert(len(res.Writes), Equals, 4)
	c.Assert(res.Writes[2].StartTs, Equals, startTs1)
	c.Assert(res.Writes[2].CommitTs, Equals, commitTs1)
	c.Assert(bytes.Compare(res.Writes[2].ShortValue, pkVal), Equals, 0)

	c.Assert(res.Writes[1].StartTs, Equals, startTs2)
	c.Assert(res.Writes[1].CommitTs, Equals, commitTs2)
	c.Assert(bytes.Compare(res.Writes[1].ShortValue, newVal), Equals, 0)

	c.Assert(res.Writes[0].StartTs, Equals, startTs4)
	c.Assert(res.Writes[0].CommitTs, Equals, commitTs4)
	c.Assert(bytes.Compare(res.Writes[0].ShortValue, emptyVal), Equals, 0)

	c.Assert(res.Writes[3].StartTs, Equals, startTs3)
	c.Assert(res.Writes[3].CommitTs, Equals, startTs3)
	c.Assert(bytes.Compare(res.Writes[3].ShortValue, []byte{0}), Equals, 0)

	// read using MvccGetByStartTs using key current ts
	res2, resKey, err := store.MvccStore.MvccGetByStartTs(store.newReqCtx(), startTs4)
	c.Assert(err, IsNil)
	c.Assert(res2, NotNil)
	c.Assert(bytes.Compare(resKey, pk), Equals, 0)
	c.Assert(len(res2.Writes), Equals, 4)
	c.Assert(res2.Writes[2].StartTs, Equals, startTs1)
	c.Assert(res2.Writes[2].CommitTs, Equals, commitTs1)
	c.Assert(bytes.Compare(res2.Writes[2].ShortValue, pkVal), Equals, 0)

	c.Assert(res2.Writes[1].StartTs, Equals, startTs2)
	c.Assert(res2.Writes[1].CommitTs, Equals, commitTs2)
	c.Assert(bytes.Compare(res2.Writes[1].ShortValue, newVal), Equals, 0)

	c.Assert(res2.Writes[0].StartTs, Equals, startTs4)
	c.Assert(res2.Writes[0].CommitTs, Equals, commitTs4)
	c.Assert(bytes.Compare(res2.Writes[0].ShortValue, emptyVal), Equals, 0)

	c.Assert(res2.Writes[3].StartTs, Equals, startTs3)
	c.Assert(res2.Writes[3].CommitTs, Equals, startTs3)
	c.Assert(bytes.Compare(res2.Writes[3].ShortValue, []byte{0}), Equals, 0)

	// read using MvccGetByStartTs using non exists startTs
	startTsNonExists := uint64(1000)
	res3, resKey, err := store.MvccStore.MvccGetByStartTs(store.newReqCtx(), startTsNonExists)
	c.Assert(err, IsNil)
	c.Assert(resKey, IsNil)
	c.Assert(res3, IsNil)

	// read using old startTs
	res4, resKey, err := store.MvccStore.MvccGetByStartTs(store.newReqCtx(), startTs2)
	c.Assert(err, IsNil)
	c.Assert(res4, NotNil)
	c.Assert(bytes.Compare(resKey, pk), Equals, 0)
	c.Assert(len(res4.Writes), Equals, 4)
	c.Assert(res4.Writes[3].StartTs, Equals, startTs3)
	c.Assert(res4.Writes[3].CommitTs, Equals, startTs3)
	c.Assert(bytes.Compare(res4.Writes[3].ShortValue, []byte{0}), Equals, 0)

	res4, resKey, err = store.MvccStore.MvccGetByStartTs(store.newReqCtxWithKeys([]byte("t1_r1"), []byte("t1_r2")), startTs2)
	c.Assert(err, IsNil)
	c.Assert(res4, NotNil)
	c.Assert(bytes.Compare(resKey, pk), Equals, 0)
	c.Assert(len(res4.Writes), Equals, 4)
	c.Assert(res4.Writes[3].StartTs, Equals, startTs3)
	c.Assert(res4.Writes[3].CommitTs, Equals, startTs3)
	c.Assert(bytes.Compare(res4.Writes[3].ShortValue, []byte{0}), Equals, 0)
}

func (s *testMvccSuite) TestPrimaryKeyOpLock(c *C) {
	store, err := NewTestStore("PrimaryKeyOpLock", "PrimaryKeyOpLock", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	pk := func() []byte { return []byte("tpk") }
	val2 := []byte("val2")
	// prewrite 100 Op_Lock
	MustPrewriteLock(pk(), pk(), 100, store)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk()}, 100, 101)
	c.Assert(err, IsNil)
	_, commitTS, _, _ := CheckTxnStatus(pk(), 100, 110, 110, false, store)
	c.Assert(commitTS, Equals, uint64(101))

	// prewrite 110 Op_Put
	err = store.MvccStore.Prewrite(store.newReqCtx(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Put, pk(), val2)},
		PrimaryLock:  pk(),
		StartVersion: 110,
		LockTtl:      100,
	})
	c.Assert(err, IsNil)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk()}, 110, 111)
	c.Assert(err, IsNil)

	// prewrite 120 Op_Lock
	MustPrewriteLock(pk(), pk(), 120, store)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk()}, 120, 121)
	c.Assert(err, IsNil)

	// the older commit record should exist
	_, commitTS, _, _ = CheckTxnStatus(pk(), 120, 130, 130, false, store)
	c.Assert(commitTS, Equals, uint64(121))
	_, commitTS, _, _ = CheckTxnStatus(pk(), 110, 130, 130, false, store)
	c.Assert(commitTS, Equals, uint64(111))
	_, commitTS, _, _ = CheckTxnStatus(pk(), 100, 130, 130, false, store)
	c.Assert(commitTS, Equals, uint64(101))

	getVal, err := store.newReqCtx().getDBReader().Get(pk(), 90)
	c.Assert(err, IsNil)
	c.Assert(getVal, IsNil)
	getVal, err = store.newReqCtx().getDBReader().Get(pk(), 110)
	c.Assert(err, IsNil)
	c.Assert(getVal, IsNil)
	getVal, err = store.newReqCtx().getDBReader().Get(pk(), 111)
	c.Assert(err, IsNil)
	c.Assert(getVal, DeepEquals, val2)
	getVal, err = store.newReqCtx().getDBReader().Get(pk(), 130)
	c.Assert(err, IsNil)
	c.Assert(getVal, DeepEquals, val2) // Op_Lock value should not be recorded and returned
}

func (s *testMvccSuite) TestMvccTxnRead(c *C) {
	store, err := NewTestStore("TestMvccTxnRead", "TestMvccTxnRead", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	// nothing at start
	k1 := []byte("tk1")
	v1 := []byte("v1")
	MustGetNone(k1, 1, store)

	// prewrite and rollback
	MustPrewriteOptimistic(k1, k1, v1, 2, 10, 2, store)
	MustRollbackKey(k1, 2, store)

	// read results in nothing
	MustGetNone(k1, 1, store)

	v1 = []byte("v1lock")
	MustPrewriteLock(k1, k1, 3, store)
	MustCommit(k1, 3, 4, store)
	// lock should left nothing
	MustGetNone(k1, 5, store)

	v := []byte("v")
	k2 := []byte("tk2")
	v2 := []byte("v2")
	MustPrewriteOptimistic(k1, k1, v, 5, 10, 5, store)
	MustPrewriteOptimistic(k1, k2, v2, 5, 10, 5, store)
	// should not be affected by later locks
	MustGetNone(k1, 4, store)
	// should read pending locks
	MustGetErr(k1, 7, store)
	// should ignore the primary lock and get none when reading the latest record
	MustGetNone(k1, maxTs, store)
	// should read secondary locks even when reading the latest record
	MustGetErr(k2, maxTs, store)
	MustCommit(k1, 5, 10, store)
	MustCommit(k2, 5, 10, store)
	MustGetNone(k1, 3, store)
	// should not read with ts < commit_ts
	MustGetNone(k1, 7, store)
	// should read with ts > commit_ts
	MustGetVal(k1, v, 13, store)
	// should read the latest record if `ts == u64::max_value()`
	MustGetVal(k2, v2, uint64(math.MaxUint64), store)

	MustPrewriteDelete(k1, k1, 15, store)
	// should ignore the lock and get previous record when reading the latest record
	MustGetVal(k1, v, maxTs, store)
	MustCommit(k1, 15, 20, store)
	MustGetNone(k1, 3, store)
	MustGetNone(k1, 7, store)
	MustGetVal(k1, v, 13, store)
	MustGetVal(k1, v, 17, store)
	MustGetNone(k1, 23, store)

	// intersecting timestamps with pessimistic txn
	// T1: start_ts = 25, commit_ts = 27
	// T2: start_ts = 23, commit_ts = 31
	MustPrewritePut(k1, k1, v, 25, store)
	MustCommit(k1, 25, 27, store)
	MustAcquirePessimisticLock(k1, k1, 23, 29, store)
	MustGetVal(k1, v, 30, store)
	MustPessimisitcPrewriteDelete(k1, k1, 23, 29, store)
	MustGetErr(k1, 30, store)
	// should read the latest record when `ts == u64::max_value()`
	// even if lock.start_ts(23) < latest write.commit_ts(27)
	MustGetVal(k1, v, maxTs, store)
	MustCommit(k1, 23, 31, store)
	MustGetVal(k1, v, 30, store)
	MustGetNone(k1, 32, store)
}

func (s *testMvccSuite) TestTxnPrewrite(c *C) {
	store, err := NewTestStore("TestTxnPrewrite", "TestTxnPrewrite", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	// nothing at start
	k := []byte("tk")
	v := []byte("v")
	MustPrewritePut(k, k, v, 5, store)
	// Key is locked.
	MustLocked(k, false, store)
	// Retry prewrite
	MustPrewritePut(k, k, v, 5, store)
	// Conflict
	MustPrewriteLockErr(k, k, v, 6, store)

	MustCommit(k, 5, 10, store)
	MustGetVal(k, v, 10, store)
	// Delayed prewrite request after committing should do nothing
	MustPrewritePutErr(k, k, v, 5, store)
	MustUnLocked(k, store)
	// Write conflict
	MustPrewritePutErr(k, k, v, 6, store)
	MustUnLocked(k, store)
	// Not conflict
	MustPrewriteLock(k, k, 12, store)
	MustLocked(k, false, store)
	MustRollbackKey(k, 12, store)
	// Cannot retry Prewrite after rollback
	MustPrewritePutErr(k, k, nil, 12, store)
	// Can prewrite after rollback
	MustPrewriteDelete(k, k, 13, store)
	MustRollbackKey(k, 13, store)
	MustUnLocked(k, store)
}
