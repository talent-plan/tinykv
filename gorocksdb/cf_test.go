package gorocksdb

import (
	"io/ioutil"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestColumnFamilyOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestColumnFamilyOpen")
	ensure.Nil(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err := OpenDbColumnFamilies(opts, dir, givenNames, []*Options{opts, opts})
	ensure.Nil(t, err)
	defer db.Close()
	ensure.DeepEqual(t, len(cfh), 2)
	cfh[0].Destroy()
	cfh[1].Destroy()

	actualNames, err := ListColumnFamilies(opts, dir)
	ensure.Nil(t, err)
	ensure.SameElements(t, actualNames, givenNames)
}

func TestColumnFamilyCreateDrop(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestColumnFamilyCreate")
	ensure.Nil(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, err := OpenDb(opts, dir)
	ensure.Nil(t, err)
	defer db.Close()
	cf, err := db.CreateColumnFamily(opts, "guide")
	ensure.Nil(t, err)
	defer cf.Destroy()

	actualNames, err := ListColumnFamilies(opts, dir)
	ensure.Nil(t, err)
	ensure.SameElements(t, actualNames, []string{"default", "guide"})

	ensure.Nil(t, db.DropColumnFamily(cf))

	actualNames, err = ListColumnFamilies(opts, dir)
	ensure.Nil(t, err)
	ensure.SameElements(t, actualNames, []string{"default"})
}

func TestColumnFamilyBatchPutGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestColumnFamilyPutGet")
	ensure.Nil(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err := OpenDbColumnFamilies(opts, dir, givenNames, []*Options{opts, opts})
	ensure.Nil(t, err)
	defer db.Close()
	ensure.DeepEqual(t, len(cfh), 2)
	defer cfh[0].Destroy()
	defer cfh[1].Destroy()

	wo := NewDefaultWriteOptions()
	defer wo.Destroy()
	ro := NewDefaultReadOptions()
	defer ro.Destroy()

	givenKey0 := []byte("hello0")
	givenVal0 := []byte("world0")
	givenKey1 := []byte("hello1")
	givenVal1 := []byte("world1")

	b0 := NewWriteBatch()
	defer b0.Destroy()
	b0.PutCF(cfh[0], givenKey0, givenVal0)
	ensure.Nil(t, db.Write(wo, b0))
	actualVal0, err := db.GetCF(ro, cfh[0], givenKey0)
	defer actualVal0.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal0.Data(), givenVal0)

	b1 := NewWriteBatch()
	defer b1.Destroy()
	b1.PutCF(cfh[1], givenKey1, givenVal1)
	ensure.Nil(t, db.Write(wo, b1))
	actualVal1, err := db.GetCF(ro, cfh[1], givenKey1)
	defer actualVal1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal1.Data(), givenVal1)

	actualVal, err := db.GetCF(ro, cfh[0], givenKey1)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)
	actualVal, err = db.GetCF(ro, cfh[1], givenKey0)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)
}

func TestColumnFamilyPutGetDelete(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestColumnFamilyPutGet")
	ensure.Nil(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err := OpenDbColumnFamilies(opts, dir, givenNames, []*Options{opts, opts})
	ensure.Nil(t, err)
	defer db.Close()
	ensure.DeepEqual(t, len(cfh), 2)
	defer cfh[0].Destroy()
	defer cfh[1].Destroy()

	wo := NewDefaultWriteOptions()
	defer wo.Destroy()
	ro := NewDefaultReadOptions()
	defer ro.Destroy()

	givenKey0 := []byte("hello0")
	givenVal0 := []byte("world0")
	givenKey1 := []byte("hello1")
	givenVal1 := []byte("world1")

	ensure.Nil(t, db.PutCF(wo, cfh[0], givenKey0, givenVal0))
	actualVal0, err := db.GetCF(ro, cfh[0], givenKey0)
	defer actualVal0.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal0.Data(), givenVal0)

	ensure.Nil(t, db.PutCF(wo, cfh[1], givenKey1, givenVal1))
	actualVal1, err := db.GetCF(ro, cfh[1], givenKey1)
	defer actualVal1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal1.Data(), givenVal1)

	actualVal, err := db.GetCF(ro, cfh[0], givenKey1)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)
	actualVal, err = db.GetCF(ro, cfh[1], givenKey0)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)

	ensure.Nil(t, db.DeleteCF(wo, cfh[0], givenKey0))
	actualVal, err = db.GetCF(ro, cfh[0], givenKey0)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, actualVal.Size(), 0)
}

func newTestDBCF(t *testing.T, name string) (db *DB, cfh []*ColumnFamilyHandle, cleanup func()) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestColumnFamilyPutGet")
	ensure.Nil(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err = OpenDbColumnFamilies(opts, dir, givenNames, []*Options{opts, opts})
	ensure.Nil(t, err)
	cleanup = func() {
		for _, cf := range cfh {
			cf.Destroy()
		}
		db.Close()
	}
	return db, cfh, cleanup
}

func TestColumnFamilyMultiGet(t *testing.T) {
	db, cfh, cleanup := newTestDBCF(t, "TestDBMultiGet")
	defer cleanup()

	var (
		givenKey1 = []byte("hello1")
		givenKey2 = []byte("hello2")
		givenKey3 = []byte("hello3")
		givenVal1 = []byte("world1")
		givenVal2 = []byte("world2")
		givenVal3 = []byte("world3")
		wo        = NewDefaultWriteOptions()
		ro        = NewDefaultReadOptions()
	)

	// create
	ensure.Nil(t, db.PutCF(wo, cfh[0], givenKey1, givenVal1))
	ensure.Nil(t, db.PutCF(wo, cfh[1], givenKey2, givenVal2))
	ensure.Nil(t, db.PutCF(wo, cfh[1], givenKey3, givenVal3))

	// column family 0 only has givenKey1
	values, err := db.MultiGetCF(ro, cfh[0], []byte("noexist"), givenKey1, givenKey2, givenKey3)
	defer values.Destroy()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(values), 4)

	ensure.DeepEqual(t, values[0].Data(), []byte(nil))
	ensure.DeepEqual(t, values[1].Data(), givenVal1)
	ensure.DeepEqual(t, values[2].Data(), []byte(nil))
	ensure.DeepEqual(t, values[3].Data(), []byte(nil))

	// column family 1 only has givenKey2 and givenKey3
	values, err = db.MultiGetCF(ro, cfh[1], []byte("noexist"), givenKey1, givenKey2, givenKey3)
	defer values.Destroy()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(values), 4)

	ensure.DeepEqual(t, values[0].Data(), []byte(nil))
	ensure.DeepEqual(t, values[1].Data(), []byte(nil))
	ensure.DeepEqual(t, values[2].Data(), givenVal2)
	ensure.DeepEqual(t, values[3].Data(), givenVal3)

	// getting them all from the right CF should return them all
	values, err = db.MultiGetCFMultiCF(ro,
		ColumnFamilyHandles{cfh[0], cfh[1], cfh[1]},
		[][]byte{givenKey1, givenKey2, givenKey3},
	)
	defer values.Destroy()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(values), 3)

	ensure.DeepEqual(t, values[0].Data(), givenVal1)
	ensure.DeepEqual(t, values[1].Data(), givenVal2)
	ensure.DeepEqual(t, values[2].Data(), givenVal3)
}
