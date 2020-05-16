package fileutil

import (
	"context"
	"os"

	"github.com/ncw/directio"
	"golang.org/x/time/rate"
)

// DirectWriter writes to a file opened with O_DIRECT flag.
// `Finish` must be called when the writing is done to truncate and sync the file.
type DirectWriter struct {
	writer
}

// BufferedWriter writes to a file with buffer.
type BufferedWriter struct {
	writer
}

type writer struct {
	fd       *os.File
	fileOff  int64
	writeBuf []byte
	bufOff   int64
	limiter  *rate.Limiter
}

func NewBufferedWriter(fd *os.File, bufSize int, limiter *rate.Limiter) *BufferedWriter {
	return &BufferedWriter{
		writer: writer{
			fd:       fd,
			writeBuf: make([]byte, bufSize),
			limiter:  limiter,
		},
	}
}

func NewDirectWriter(fd *os.File, bufSize int, limiter *rate.Limiter) *DirectWriter {
	return &DirectWriter{
		writer: writer{
			fd:       fd,
			writeBuf: directio.AlignedBlock(bufSize),
			limiter:  limiter,
		},
	}
}

func (l *writer) Reset(fd *os.File) {
	l.fd = fd
	l.fileOff = 0
	l.bufOff = 0
}

func (l *writer) Write(p []byte) (n int, err error) {
	return len(p), l.Append(p)
}

func (l *writer) Append(val []byte) error {
	for {
		n := copy(l.writeBuf[l.bufOff:], val)
		l.bufOff += int64(n)
		if n == len(val) {
			return nil
		}
		err := l.flush()
		if err != nil {
			return err
		}
		val = val[n:]
	}
}

func (l *writer) waitRateLimiter() {
	if l.limiter != nil {
		err := l.limiter.WaitN(context.Background(), int(l.bufOff))
		if err != nil {
			panic(err)
		}
	}
}

func (l *writer) flush() error {
	if l.bufOff == 0 {
		return nil
	}
	l.waitRateLimiter()
	_, err := l.fd.Write(l.writeBuf[:l.bufOff])
	if err != nil {
		return err
	}
	l.fileOff += l.bufOff
	l.bufOff = 0
	return nil
}

func (l *writer) Offset() int64 {
	return l.fileOff + l.bufOff
}

func (l *BufferedWriter) Flush() error {
	return l.flush()
}

func (l *BufferedWriter) Sync() error {
	return Fdatasync(l.fd)
}

func (l *DirectWriter) Finish() error {
	if l.bufOff == 0 {
		return nil
	}
	finalLength := l.fileOff + l.bufOff
	l.bufOff = alignedSize(l.bufOff)
	err := l.flush()
	if err != nil {
		return err
	}
	l.fileOff = finalLength
	err = l.fd.Truncate(finalLength)
	if err != nil {
		return err
	}
	return Fdatasync(l.fd)
}

func alignedSize(n int64) int64 {
	return (n + directio.BlockSize - 1) / directio.BlockSize * directio.BlockSize
}
