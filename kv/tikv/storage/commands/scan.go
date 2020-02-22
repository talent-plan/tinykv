package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Scan struct {
	ReadOnly
	CommandBase
	request  *kvrpcpb.ScanRequest
	response *kvrpcpb.ScanResponse
}

func NewScan(request *kvrpcpb.ScanRequest) Scan {
	response := new(kvrpcpb.ScanResponse)
	result := Scan{
		CommandBase: CommandBase{
			context:  request.Context,
			response: response,
		},
		request:  request,
		response: response,
	}
	return result
}

func (s *Scan) BuildTxn(txn *kvstore.MvccTxn) error {
	txn.StartTS = &s.request.Version

	scanner := kvstore.NewScanner(s.request.StartKey, txn)
	limit := s.request.Limit
	for {
		if limit == 0 {
			// We've scanned up to the requested limit.
			return nil
		}
		limit -= 1

		key, value, err := scanner.Next()
		if err != nil {
			// Key error (e.g., key is locked) is saved as an error in the scan for the client to handle.
			if e, ok := err.(KeyError); ok {
				keyErrs := e.KeyErrors()
				if len(keyErrs) == 0 {
					pair := kvrpcpb.KvPair{}
					pair.Error = keyErrs[0]
					s.response.Pairs = append(s.response.Pairs, &pair)
					continue
				}
			}
			// Any other kind of error, we can't handle so quit the scan.
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
