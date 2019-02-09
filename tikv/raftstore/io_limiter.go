package raftstore

import "io"

type IOLimiter struct {
}

type LimitWriter struct {
	limiter *IOLimiter
	writer  io.Writer
}

func (lw *LimitWriter) Write(b []byte) (int, error) {
	return lw.writer.Write(b)
}
