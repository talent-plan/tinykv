package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
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
			startTs: request.Version,
		},
		request: request,
	}
	return result
}

func (s *Scan) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	response := new(kvrpcpb.ScanResponse)

	scanner := mvcc.NewScanner(s.request.StartKey, txn)
	defer scanner.Close()
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
			if e, ok := err.(*mvcc.KeyError); ok {
				pair := new(kvrpcpb.KvPair)
				pair.Error = &e.KeyError
				response.Pairs = append(response.Pairs, pair)
				continue
			} else {
				// Any other kind of error, we can't handle so quit the scan.
				return nil, nil, err
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
