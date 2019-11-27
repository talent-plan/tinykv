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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

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

func TestMvcc(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMvccSuite{})

type testMvccSuite struct{}

func (s *testMvccSuite) TestBasicOptimistic(c *C) {
	var err error
	store, err := NewTestStore("basic_optimistic_db", "basic_optimistic_log")
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}

	key1 := []byte("key1")
	val1 := []byte("val1")
	prewriteMut := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Op_Put,
		Key:   key1,
		Value: val1,
	}
	ttl := uint64(200)
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{prewriteMut},
		PrimaryLock:  key1,
		StartVersion: uint64(1),
		LockTtl:      ttl,
	}
	err = store.MvccStore.prewriteOptimistic(reqCtx, prewriteReq.Mutations, prewriteReq)
	c.Assert(err, IsNil)

	lock := store.MvccStore.getLock(reqCtx, key1)
	c.Assert(uint64(lock.TTL), Equals, uint64(200))
	c.Assert(bytes.Compare(lock.Value, val1), Equals, 0)

	err = store.MvccStore.Commit(reqCtx, [][]byte{key1}, uint64(1), uint64(2))
	c.Assert(err, IsNil)

	rCtx1 := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	getVal, err := rCtx1.getDBReader().Get(key1, uint64(3))
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(getVal, val1), Equals, 0)

	rCtx2 := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	getVal, err = rCtx2.getDBReader().Get(key1, uint64(1))
	c.Assert(err, IsNil)
	c.Assert(getVal, IsNil)
}

func (s *testMvccSuite) TestPessimiticTxnTTL(c *C) {
	var err error
	store, err := NewTestStore("pessimisitc_txn_ttl_db", "pessimisitc_txn_ttl_log")
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	// Pessimisitc lock key1
	key1 := []byte("key1")
	val1 := []byte("")
	mut := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Op_PessimisticLock,
		Key:   key1,
		Value: val1,
	}
	reqCtx := &requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.Svr,
	}
	ttl := uint64(1000)
	req := &kvrpcpb.PessimisticLockRequest{
		Mutations:    []*kvrpcpb.Mutation{mut},
		PrimaryLock:  key1,
		StartVersion: uint64(1),
		LockTtl:      ttl,
		ForUpdateTs:  ttl,
		IsFirstLock:  true,
	}
	_, err = store.MvccStore.PessimisticLock(reqCtx, req)
	c.Assert(err, IsNil)

	// Prewrite key1 with smaller lock ttl
	val1 = []byte("val1")
	mut = &kvrpcpb.Mutation{
		Op:    kvrpcpb.Op_Put,
		Key:   key1,
		Value: val1,
	}
	isPessLock := make([]bool, 1)
	isPessLock[0] = true
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:         []*kvrpcpb.Mutation{mut},
		PrimaryLock:       key1,
		StartVersion:      uint64(1),
		LockTtl:           ttl - 500,
		IsPessimisticLock: isPessLock,
		ForUpdateTs:       ttl,
	}
	err = store.MvccStore.prewritePessimistic(reqCtx, req.Mutations, prewriteReq)
	c.Assert(err, IsNil)

	lock := store.MvccStore.getLock(reqCtx, key1)
	c.Assert(uint64(lock.TTL), Equals, uint64(1000))
}
