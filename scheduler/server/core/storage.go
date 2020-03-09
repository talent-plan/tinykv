// Copyright 2017 PingCAP, Inc.
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
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
)

const (
	clusterPath  = "raft"
	schedulePath = "schedule"
	gcPath       = "gc"

	customScheduleConfigPath = "scheduler_config"
)

const (
	maxKVRangeLimit = 10000
	minKVRangeLimit = 100
)

// Storage wraps all kv operations, keep it stateless.
type Storage struct {
	kv.Base
}

// NewStorage creates Storage instance with Base.
func NewStorage(base kv.Base) *Storage {
	return &Storage{
		Base: base,
	}
}

func (s *Storage) storePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func regionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

// ClusterStatePath returns the path to save an option.
func (s *Storage) ClusterStatePath(option string) string {
	return path.Join(clusterPath, "status", option)
}

func (s *Storage) storeLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

func (s *Storage) storeRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

// SaveScheduleConfig saves the config of scheduler.
func (s *Storage) SaveScheduleConfig(scheduleName string, data []byte) error {
	configPath := path.Join(customScheduleConfigPath, scheduleName)
	return s.Save(configPath, string(data))
}

// RemoveScheduleConfig remvoes the config of scheduler.
func (s *Storage) RemoveScheduleConfig(scheduleName string) error {
	configPath := path.Join(customScheduleConfigPath, scheduleName)
	return s.Remove(configPath)
}

// LoadScheduleConfig loads the config of scheduler.
func (s *Storage) LoadScheduleConfig(scheduleName string) (string, error) {
	configPath := path.Join(customScheduleConfigPath, scheduleName)
	return s.Load(configPath)
}

// LoadMeta loads cluster meta from storage.
func (s *Storage) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return loadProto(s.Base, clusterPath, meta)
}

// SaveMeta save cluster meta to storage.
func (s *Storage) SaveMeta(meta *metapb.Cluster) error {
	return saveProto(s.Base, clusterPath, meta)
}

// LoadStore loads one store from storage.
func (s *Storage) LoadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return loadProto(s.Base, s.storePath(storeID), store)
}

// SaveStore saves one store to storage.
func (s *Storage) SaveStore(store *metapb.Store) error {
	return saveProto(s.Base, s.storePath(store.GetId()), store)
}

// DeleteStore deletes one store from storage.
func (s *Storage) DeleteStore(store *metapb.Store) error {
	return s.Remove(s.storePath(store.GetId()))
}

// LoadStores loads all stores from storage to StoresInfo.
func (s *Storage) LoadStores(f func(store *StoreInfo)) error {
	nextID := uint64(0)
	endKey := s.storePath(math.MaxUint64)
	for {
		key := s.storePath(nextID)
		_, res, err := s.LoadRange(key, endKey, minKVRangeLimit)
		if err != nil {
			return err
		}
		for _, str := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(str)); err != nil {
				return errors.WithStack(err)
			}
			leaderWeight, err := s.loadFloatWithDefaultValue(s.storeLeaderWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			regionWeight, err := s.loadFloatWithDefaultValue(s.storeRegionWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			newStoreInfo := NewStoreInfo(store, SetLeaderWeight(leaderWeight), SetRegionWeight(regionWeight))

			nextID = store.GetId() + 1
			f(newStoreInfo)
		}
		if len(res) < minKVRangeLimit {
			return nil
		}
	}
}

// SaveStoreWeight saves a store's leader and region weight to storage.
func (s *Storage) SaveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := s.Save(s.storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return err
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	return s.Save(s.storeRegionWeightPath(storeID), regionValue)
}

func (s *Storage) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := s.Load(path)
	if err != nil {
		return 0, err
	}
	if res == "" {
		return def, nil
	}
	val, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return val, nil
}

// Flush flushes the dirty region to storage.
func (s *Storage) Flush() error {
	return nil
}

// Close closes the s.
func (s *Storage) Close() error {
	return nil
}

// SaveGCSafePoint saves new GC safe point to storage.
func (s *Storage) SaveGCSafePoint(safePoint uint64) error {
	key := path.Join(gcPath, "safe_point")
	value := strconv.FormatUint(safePoint, 16)
	return s.Save(key, value)
}

// LoadGCSafePoint loads current GC safe point from storage.
func (s *Storage) LoadGCSafePoint() (uint64, error) {
	key := path.Join(gcPath, "safe_point")
	value, err := s.Load(key)
	if err != nil {
		return 0, err
	}
	if value == "" {
		return 0, nil
	}
	safePoint, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, err
	}
	return safePoint, nil
}

// LoadAllScheduleConfig loads all schedulers' config.
func (s *Storage) LoadAllScheduleConfig() ([]string, []string, error) {
	keys, values, err := s.LoadRange(customScheduleConfigPath, clientv3.GetPrefixRangeEnd(customScheduleConfigPath), 1000)
	for i, key := range keys {
		keys[i] = strings.TrimPrefix(key, customScheduleConfigPath+"/")
	}
	return keys, values, err
}

func loadProto(s kv.Base, key string, msg proto.Message) (bool, error) {
	value, err := s.Load(key)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = proto.Unmarshal([]byte(value), msg)
	return true, errors.WithStack(err)
}

func saveProto(s kv.Base, key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	return s.Save(key, string(value))
}
