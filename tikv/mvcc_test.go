package tikv

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/coocood/badger"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/util/lockwaiter"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

var _ = Suite(&testMvccSuite{})

type testMvccSuite struct{}

type TestStore struct {
	MvccStore *MVCCStore
	Svr       *Server
	DBPath    string
	LogPath   string
}

func CreateTestDB(dbPath, LogPath string) (*badger.DB, error) {
	subPath := fmt.Sprintf("/%d", 0)
	opts := badger.DefaultOptions
	opts.Dir = dbPath + subPath
	opts.ValueDir = LogPath + subPath
	return badger.Open(opts)
}

func NewTestStore(dbPrefix string, logPrefix string) (*TestStore, error) {
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
	writer := NewDBWriter(dbBundle, safePoint)
	store := NewMVCCStore(dbBundle, dbPath, safePoint, writer, nil)
	svr := NewServer(nil, store, nil)
	return &TestStore{
		MvccStore: store,
		Svr:       svr,
		DBPath:    dbPath,
		LogPath:   LogPath,
	}, nil
}

func CleanTestStore(store *TestStore) {
	os.RemoveAll(store.DBPath)
	os.RemoveAll(store.LogPath)
}

// PessimisticLock will add pessimistic lock on key
func PessimisticLock(pk []byte, key []byte, startTs uint64, lockTTL uint64, forUpdateTs uint64,
	isFirstLock bool, store *TestStore) (*lockwaiter.Waiter, error) {
	mut := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Op_PessimisticLock,
		Key:   key,
		Value: []byte(""),
	}
	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	req := &kvrpcpb.PessimisticLockRequest{
		Mutations:    []*kvrpcpb.Mutation{mut},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
		ForUpdateTs:  forUpdateTs,
		IsFirstLock:  isFirstLock,
	}
	waiter, err := store.MvccStore.PessimisticLock(reqCtx, req)
	return waiter, err
}

// PrewriteOptimistic raises optimistic prewrite requests on store
func PrewriteOptimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64, store *TestStore) error {
	var err error
	mut := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Op_Put,
		Key:   key,
		Value: value,
	}
	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{mut},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
	}
	err = store.MvccStore.prewriteOptimistic(reqCtx, prewriteReq.Mutations, prewriteReq)
	return err
}

// PrewritePessimistic raises pessmistic prewrite requests
func PrewritePessimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	isPessimisticLock []bool, forUpdateTs uint64, store *TestStore) error {
	var err error
	mut := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Op_Put,
		Key:   key,
		Value: value,
	}
	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:         []*kvrpcpb.Mutation{mut},
		PrimaryLock:       pk,
		StartVersion:      startTs,
		LockTtl:           lockTTL,
		IsPessimisticLock: isPessimisticLock,
		ForUpdateTs:       forUpdateTs,
	}
	err = store.MvccStore.prewritePessimistic(reqCtx, prewriteReq.Mutations, prewriteReq)
	return err
}

// CommitKey will commit key specified
func CommitKey(key []byte, startTs, commitTs uint64, store *TestStore) error {
	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	err := store.MvccStore.Commit(reqCtx, [][]byte{key}, startTs, commitTs)
	return err
}

func KvGet(key []byte, readTs uint64, store *TestStore) ([]byte, error) {
	rCtx1 := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	getVal, err := rCtx1.getDBReader().Get(key, readTs)
	return getVal, err
}

func RollBackKey(key []byte, startTs uint64, store *TestStore) error {
	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	err := store.MvccStore.Rollback(reqCtx, [][]byte{key}, startTs)
	return err
}

func MustPrewriteOptimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64, store *TestStore, c *C) {
	c.Assert(PrewriteOptimistic(pk, key, value, startTs, lockTTL, store), IsNil)
	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	lock := store.MvccStore.getLock(reqCtx, key)
	c.Assert(uint64(lock.TTL), Equals, lockTTL)
	c.Assert(bytes.Compare(lock.Value, value), Equals, 0)
}

func MustPrewritePessimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	isPessimisticLock []bool, forUpdateTs uint64, store *TestStore, c *C) {
	c.Assert(PrewritePessimistic(pk, key, value, startTs, lockTTL, isPessimisticLock, forUpdateTs, store), IsNil)
	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	lock := store.MvccStore.getLock(reqCtx, key)
	c.Assert(lock.ForUpdateTS, Equals, forUpdateTs)
	c.Assert(bytes.Compare(lock.Value, value), Equals, 0)
}

func MustCommitKey(key, val []byte, startTs, commitTs uint64, store *TestStore, c *C) {
	err := CommitKey(key, startTs, commitTs, store)
	c.Assert(err, IsNil)
	c.Assert(err, IsNil)
	getVal, err := KvGet(key, commitTs, store)
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(getVal, val), Equals, 0)
}

func MustRollbackKey(key []byte, startTs uint64, store *TestStore, c *C) {
	err := RollBackKey(key, startTs, store)
	c.Assert(err, IsNil)
	rollbackKey := mvcc.EncodeRollbackKey(nil, key, startTs)
	res := store.MvccStore.rollbackStore.Get(rollbackKey, nil)
	c.Assert(bytes.Compare(res, []byte{0}), Equals, 0)
}

func TestMvcc(t *testing.T) {
	TestingT(t)
}
func (s *testMvccSuite) TestBasicOptimistic(c *C) {
	var err error
	store, err := NewTestStore("basic_optimistic_db", "basic_optimistic_log")
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	key1 := []byte("key1")
	val1 := []byte("val1")
	ttl := uint64(200)
	MustPrewriteOptimistic(key1, key1, val1, 1, ttl, store, c)
	MustCommitKey(key1, val1, 1, 2, store, c)
	// Read using smaller ts results in nothing
	getVal, err := KvGet(key1, 1, store)
	c.Assert(getVal, IsNil)
}

func (s *testMvccSuite) TestPessimiticTxnTTL(c *C) {
	var err error
	store, err := NewTestStore("pessimisitc_txn_ttl_db", "pessimisitc_txn_ttl_log")
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
	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	MustPrewritePessimistic(key1, key1, val1, startTs, lockTTL-500, []bool{true}, startTs, store, c)
	lock := store.MvccStore.getLock(reqCtx, key1)
	c.Assert(uint64(lock.TTL), Equals, uint64(1000))

	key2 := []byte("key2")
	val2 := []byte("val2")
	_, err = PessimisticLock(key2, key2, 3, 300, 3, true, store)
	c.Assert(err, IsNil)

	// Prewrite key1 with larger lock ttl, lock ttl will be updated
	MustPrewritePessimistic(key2, key2, val2, 3, 2000, []bool{true}, 3, store, c)
	lock2 := store.MvccStore.getLock(reqCtx, key2)
	c.Assert(uint64(lock2.TTL), Equals, uint64(2000))
}

func (s *testMvccSuite) TestRollback(c *C) {
	var err error
	store, err := NewTestStore("RollbackData", "RollbackLog")
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	key := []byte("key")
	val := []byte("value")
	startTs := uint64(1)
	lockTTL := uint64(100)
	// Add a Rollback whose start ts is 1.
	MustPrewriteOptimistic(key, key, val, startTs, lockTTL, store, c)
	MustRollbackKey(key, startTs, store, c)

	MustPrewriteOptimistic(key, key, val, startTs+1, lockTTL, store, c)
	MustRollbackKey(key, startTs+1, store, c)
	var buf []byte
	// Rollback entry still exits in rollbackStore if no rollbackGC
	rollbackKey := mvcc.EncodeRollbackKey(buf, key, startTs)
	res := store.MvccStore.rollbackStore.Get(rollbackKey, nil)
	c.Assert(bytes.Compare(res, []byte{0}), Equals, 0)
}
