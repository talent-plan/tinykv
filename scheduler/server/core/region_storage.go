// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var dirtyFlushTick = time.Second

// RegionStorage is used to save regions.
type RegionStorage struct {
	*kv.LeveldbKV
	mu                  sync.RWMutex
	batchRegions        map[string]*metapb.Region
	batchSize           int
	cacheSize           int
	flushRate           time.Duration
	flushTime           time.Time
	regionStorageCtx    context.Context
	regionStorageCancel context.CancelFunc
}

const (
	//DefaultFlushRegionRate is the ttl to sync the regions to region storage.
	defaultFlushRegionRate = 3 * time.Second
	//DefaultBatchSize is the batch size to save the regions to region storage.
	defaultBatchSize = 100
)

// NewRegionStorage returns a region storage that is used to save regions.
func NewRegionStorage(ctx context.Context, path string) (*RegionStorage, error) {
	levelDB, err := kv.NewLeveldbKV(path)
	if err != nil {
		return nil, err
	}
	regionStorageCtx, regionStorageCancel := context.WithCancel(ctx)
	s := &RegionStorage{
		LeveldbKV:           levelDB,
		batchSize:           defaultBatchSize,
		flushRate:           defaultFlushRegionRate,
		batchRegions:        make(map[string]*metapb.Region, defaultBatchSize),
		flushTime:           time.Now().Add(defaultFlushRegionRate),
		regionStorageCtx:    regionStorageCtx,
		regionStorageCancel: regionStorageCancel,
	}
	s.backgroundFlush()
	return s, nil
}

func (s *RegionStorage) backgroundFlush() {
	ticker := time.NewTicker(dirtyFlushTick)
	var (
		isFlush bool
		err     error
	)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.mu.RLock()
				isFlush = s.flushTime.Before(time.Now())
				s.mu.RUnlock()
				if !isFlush {
					continue
				}
				if err = s.FlushRegion(); err != nil {
					log.Error("flush regions meet error", zap.Error(err))
				}
			case <-s.regionStorageCtx.Done():
				return
			}
		}
	}()
}

// SaveRegion saves one region to storage.
func (s *RegionStorage) SaveRegion(region *metapb.Region) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cacheSize < s.batchSize-1 {
		s.batchRegions[regionPath(region.GetId())] = region
		s.cacheSize++

		s.flushTime = time.Now().Add(s.flushRate)
		return nil
	}
	s.batchRegions[regionPath(region.GetId())] = region
	err := s.flush()

	if err != nil {
		return err
	}
	return nil
}

func deleteRegion(kv kv.Base, region *metapb.Region) error {
	return kv.Remove(regionPath(region.GetId()))
}

func loadRegions(kv kv.Base, f func(region *RegionInfo) []*RegionInfo) error {
	nextID := uint64(0)
	endKey := regionPath(math.MaxUint64)

	// Since the region key may be very long, using a larger rangeLimit will cause
	// the message packet to exceed the grpc message size limit (4MB). Here we use
	// a variable rangeLimit to work around.
	rangeLimit := maxKVRangeLimit
	for {
		startKey := regionPath(nextID)
		_, res, err := kv.LoadRange(startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= minKVRangeLimit {
				continue
			}
			return err
		}

		for _, s := range res {
			region := &metapb.Region{}
			if err := region.Unmarshal([]byte(s)); err != nil {
				return errors.WithStack(err)
			}

			nextID = region.GetId() + 1
			overlaps := f(NewRegionInfo(region, nil))
			for _, item := range overlaps {
				if err := deleteRegion(kv, item.GetMeta()); err != nil {
					return err
				}
			}
		}

		if len(res) < rangeLimit {
			return nil
		}
	}
}

// FlushRegion saves the cache region to region storage.
func (s *RegionStorage) FlushRegion() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flush()
}

func (s *RegionStorage) flush() error {
	if err := s.SaveRegions(s.batchRegions); err != nil {
		return err
	}
	s.cacheSize = 0
	s.batchRegions = make(map[string]*metapb.Region, s.batchSize)
	return nil
}

// Close closes the kv.
func (s *RegionStorage) Close() error {
	err := s.FlushRegion()
	if err != nil {
		log.Error("meet error before close the region storage", zap.Error(err))
	}
	s.regionStorageCancel()
	return errors.WithStack(s.LeveldbKV.Close())
}
