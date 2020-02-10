package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Scan struct {
	request  *kvrpcpb.ScanRequest
	response *kvrpcpb.ScanResponse
}

func NewScan(request *kvrpcpb.ScanRequest) Scan {
	return Scan{request, &kvrpcpb.ScanResponse{}}
}

func (s *Scan) BuildTxn(txn *kvstore.MvccTxn) error {
	txn.StartTS = &s.request.Version

	iter := kvstore.NewScanner(s.request.StartKey, txn)
	limit := s.request.Limit
	for {
		if limit == 0 {
			// We've scanned up to the requested limit.
			return nil
		}
		limit -= 1
		key, value, err := iter.Next()
		if err != nil {
			return err
		}
		if key == nil {
			// Reached the end of the DB
			return nil
		}

		pair := kvrpcpb.KvPair{}
		pair.Key = key
		pair.Value = value
		s.response.Pairs = append(s.response.Pairs, &pair)
	}
}

func (s *Scan) Context() *kvrpcpb.Context {
	return s.request.Context
}

func (s *Scan) Response() interface{} {
	return s.response
}

func (s *Scan) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		s.response.RegionError = regionErr
		return &s.response
	}

	return nil
}
