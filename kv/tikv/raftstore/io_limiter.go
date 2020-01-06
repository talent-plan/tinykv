package raftstore

import (
	"io"

	"golang.org/x/time/rate"
)

type IOLimiter = rate.Limiter

func NewIOLimiter(rateLimit int) *IOLimiter {
	return rate.NewLimiter(rate.Limit(rateLimit), rateLimit)
}

func NewInfLimiter() *IOLimiter {
	return rate.NewLimiter(rate.Inf, 0)
}

type LimitWriter struct {
	limiter *IOLimiter
	writer  io.Writer
}

func (lw *LimitWriter) Write(b []byte) (int, error) {
	return lw.writer.Write(b)
}
