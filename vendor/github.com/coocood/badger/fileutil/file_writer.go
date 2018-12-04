package fileutil

import (
	"context"
	"golang.org/x/time/rate"
	"os"
)

type BufferedFileWriter struct {
	f              *os.File
	buf            []byte
	fileSize       int64
	bufSize        int64
	bytesPerSync   int64
	lastSyncOffset int64

	limiter *rate.Limiter
}

// NewBufferedFileWriter makes a new NewBufferedFileWriter.
// BufferedFileWriter use SyncFileRange internally, you can control the this sync by bytesPerSync.
// If the limiter is nil, this writer will not limit the write speed.
func NewBufferedFileWriter(f *os.File, bufSize int, bytesPerSync int, limiter *rate.Limiter) *BufferedFileWriter {
	return &BufferedFileWriter{
		f:            f,
		buf:          make([]byte, 0, bufSize),
		bufSize:      int64(bufSize),
		bytesPerSync: int64(bytesPerSync),
		limiter:      limiter,
	}
}

// Append data to buffer, flush buffer if needed.
// If bytesPerSync > 0, this call may flush some dirty page respect to bytesPerSync.
func (bw *BufferedFileWriter) Append(data []byte) error {
	return bw.appendData(data, true)
}

// Flush data in buffer to disk.
// If fsync is true, it will do a Fdatasync, otherwise it will try to do a SyncFileRange.
func (bw *BufferedFileWriter) Flush(fsync bool) error {
	if err := bw.flushBuffer(!fsync); err != nil {
		return err
	}
	if fsync {
		return bw.fdatasync()
	}
	return nil
}

// FlushWithData append data to buffer and then do a buffer flush just like Flush.
func (bw *BufferedFileWriter) FlushWithData(data []byte, fsync bool) error {
	if err := bw.appendData(data, !fsync); err != nil {
		return err
	}
	if err := bw.flushBuffer(!fsync); err != nil {
		return err
	}
	if fsync {
		return bw.fdatasync()
	}
	return nil
}

// Reset this writer with new file.
func (bw *BufferedFileWriter) Reset(f *os.File) {
	bw.f = f
	bw.buf = bw.buf[:0]
	bw.fileSize = 0
	bw.lastSyncOffset = 0
}

// Reset this writer with new file and rate limiter.
func (bw *BufferedFileWriter) ResetWithLimiter(f *os.File, limiter *rate.Limiter) {
	bw.Reset(f)
	bw.limiter = limiter
}

func (bw *BufferedFileWriter) write(data []byte, syncRange bool) error {
	for cur := 0; cur < len(data); {
		allowed := bw.requestTokenForWrite(len(data) - cur)
		if _, err := bw.f.Write(data[cur : cur+allowed]); err != nil {
			return err
		}
		bw.fileSize += int64(allowed)
		if syncRange {
			if err := bw.trySyncFileRange(); err != nil {
				return err
			}
		}
		cur += allowed
	}
	return nil
}

func (bw *BufferedFileWriter) flushBuffer(syncRange bool) error {
	if len(bw.buf) == 0 {
		return nil
	}
	if err := bw.write(bw.buf, syncRange); err != nil {
		return err
	}
	bw.buf = bw.buf[:0]
	return nil
}

func (bw *BufferedFileWriter) appendData(data []byte, syncRange bool) error {
	if cap(bw.buf)-len(bw.buf) < len(data) && len(bw.buf) > 0 {
		if err := bw.flushBuffer(syncRange); err != nil {
			return err
		}
	}
	if cap(bw.buf) >= len(data) {
		bw.buf = append(bw.buf, data...)
		return nil
	}
	return bw.write(data, syncRange)
}

func (bw *BufferedFileWriter) trySyncFileRange() error {
	var err error
	const bytesAlign = 4 * 1024
	const bytesNotSync = 1024 * 1024
	if bw.bytesPerSync != 0 && bw.fileSize > bytesNotSync {
		// We try to avoid sync the last 1mb of data. For two reasons:
		// 1. avoid rewrite the same page that is modified later.
		// 2. write may block while writing out the page in some OS.
		syncOffset := bw.fileSize - bytesNotSync
		syncOffset -= syncOffset % bytesAlign
		syncSize := syncOffset - bw.lastSyncOffset
		if syncOffset > 0 && syncSize >= bw.bytesPerSync {
			err = SyncFileRange(bw.f, bw.lastSyncOffset, syncSize, true)
			bw.lastSyncOffset = syncOffset
		}
	}
	return err
}

func (bw *BufferedFileWriter) fdatasync() error {
	bw.lastSyncOffset = bw.fileSize
	return Fdatasync(bw.f)
}

func (bw *BufferedFileWriter) requestTokenForWrite(size int) int {
	if bw.limiter == nil {
		return size
	}

	n, max := size, bw.limiter.Burst()
	if max != 0 && size > max {
		n = max
	}
	bw.limiter.WaitN(context.Background(), n)
	return n
}
