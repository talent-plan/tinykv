package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Scan struct {
	ReadOnly
	CommandBase
	request *kvrpcpb.ScanRequest
}

func NewScan(request *kvrpcpb.ScanRequest) Scan {
	result := Scan{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
	return result
}

func (s *Scan) Read(txn *kvstore.RoTxn) (interface{}, [][]byte, error) {
	txn.StartTS = &s.request.Version
	response := new(kvrpcpb.ScanResponse)

	scanner := kvstore.NewScanner(s.request.StartKey, txn)
	limit := s.request.Limit
	for {
		if limit == 0 {
			// We've scanned up to the requested limit.
			return response, nil, nil
		}
		limit -= 1

		key, value, err := scanner.Next()
		if err != nil {
			// Key error (e.g., key is locked) is saved as an error in the scan for the client to handle.
			if e, ok := err.(*kvrpcpb.KeyError); ok {
				pair := new(kvrpcpb.KvPair)
				pair.Error = e
				response.Pairs = append(response.Pairs, pair)
				continue
			} else if e, ok := err.(error); ok {
				// Any other kind of error, we can't handle so quit the scan.
				return regionErrorRo(e, response)
			} else {
				// No way we should get here.
				continue
			}
		}
		if key == nil {
			// Reached the end of the DB
			return response, nil, nil
		}

		pair := kvrpcpb.KvPair{}
		pair.Key = key
		pair.Value = value
		response.Pairs = append(response.Pairs, &pair)
	}
}
