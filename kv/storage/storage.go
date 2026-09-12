package storage

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Storage represents the internal-facing server part of TinyKV, it handles sending and receiving from other
// TinyKV nodes. As part of that responsibility, it also reads and writes data to disk (or semi-permanent memory).
type Storage interface {
	Start() error
	Stop() error
	// должен быть атомарным, те если хотя бы одна запись с ошибкой, то выкидывать ошибку
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	// должен возвращать объект, который будет читать снэпшот. снэпшот, потому что нам нужно поддержать отсутсвие аномалий
	Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}

type StorageReader interface {
	// When the key doesn't exist, return nil for the value
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
	Close()
}
