package rocksdb

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	smallTestSize = 10
	largeTestSize = 50000
)

func TestNoCompression(t *testing.T) {
	opts := NewDefaultBlockBasedTableOptions(bytes.Compare)
	t.Run("small", func(t *testing.T) {
		testSstReadWrite(t, smallTestSize, opts)
	})
	t.Run("large", func(t *testing.T) {
		testSstReadWrite(t, largeTestSize, opts)
	})
}

func TestLz4Compression(t *testing.T) {
	opts := NewDefaultBlockBasedTableOptions(bytes.Compare)
	opts.CompressionType = CompressionLz4

	t.Run("small", func(t *testing.T) {
		testSstReadWrite(t, smallTestSize, opts)
	})
	t.Run("large", func(t *testing.T) {
		testSstReadWrite(t, largeTestSize, opts)
	})
}

func TestBlockAlign(t *testing.T) {
	opts := NewDefaultBlockBasedTableOptions(bytes.Compare)
	opts.CompressionType = CompressionLz4
	opts.BlockAlign = true

	t.Run("small", func(t *testing.T) {
		testSstReadWrite(t, smallTestSize, opts)
	})
	t.Run("large", func(t *testing.T) {
		testSstReadWrite(t, largeTestSize, opts)
	})
}

func TestNoChecksum(t *testing.T) {
	opts := NewDefaultBlockBasedTableOptions(bytes.Compare)
	opts.ChecksumType = ChecksumNone

	t.Run("small", func(t *testing.T) {
		testSstReadWrite(t, smallTestSize, opts)
	})
	t.Run("large", func(t *testing.T) {
		testSstReadWrite(t, largeTestSize, opts)
	})
}

func testSstReadWrite(t *testing.T, num int, opts *BlockBasedTableOptions) {
	nums := sortedNumbers(num)
	f, err := ioutil.TempFile("", "unistore-test.*.sst")
	require.Nil(t, err)
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()

	w := NewSstFileWriter(f, opts)
	for _, num := range nums {
		err := w.Put([]byte(num), []byte(num))
		require.Nil(t, err)
	}
	require.Nil(t, w.Finish())

	it, err := NewSstFileIterator(f)
	require.Nil(t, err)
	for n := 0; n < 2; n++ {
		var i int
		for it.SeekToFirst(); it.Valid(); it.Next() {
			key := it.Key()
			value := string(it.Value())

			require.Equal(t, nums[i], string(key.UserKey))
			require.Equal(t, nums[i], string(value))
			i++
		}
		require.Equal(t, num, i)
		require.Nil(t, it.Err())
	}
}
