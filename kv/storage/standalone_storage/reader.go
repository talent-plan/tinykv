package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type reader struct {
	storage   *StandAloneStorage
	iterCount int
}

func (r *reader) GetCF(cf string, key []byte) ([]byte, error) {
	return nil, nil
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	return nil
}

func (r *reader) Close() {
	return
}
