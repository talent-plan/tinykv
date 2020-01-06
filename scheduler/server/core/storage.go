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
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
)

const (
	clusterPath  = "raft"
	configPath   = "config"
	schedulePath = "schedule"
	gcPath       = "gc"
	rulesPath    = "rules"

	customScheduleConfigPath = "scheduler_config"
)

const (
	maxKVRangeLimit = 10000
	minKVRangeLimit = 100
)

// Storage wraps all kv operations, keep it stateless.
type Storage struct {
	kv.Base
	regionStorage    *RegionStorage
	useRegionStorage int32
}

// NewStorage creates Storage instance with Base.
func NewStorage(base kv.Base) *Storage {
	return &Storage{
		Base: base,
	}
}

// SetRegionStorage sets the region storage.
func (s *Storage) SetRegionStorage(regionStorage *RegionStorage) *Storage {
	s.regionStorage = regionStorage
	return s
}

// GetRegionStorage gets the region storage.
func (s *Storage) GetRegionStorage() *RegionStorage {
	return s.regionStorage
}

// SwitchToRegionStorage switches to the region storage.
func (s *Storage) SwitchToRegionStorage() {
	atomic.StoreInt32(&s.useRegionStorage, 1)
}

// SwitchToDefaultStorage switches to the to default storage.
func (s *Storage) SwitchToDefaultStorage() {
	atomic.StoreInt32(&s.useRegionStorage, 0)
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

// LoadRegion loads one regoin from storage.
func (s *Storage) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return loadProto(s.regionStorage, regionPath(regionID), region)
	}
	return loadProto(s.Base, regionPath(regionID), region)
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (s *Storage) LoadRegions(f func(region *RegionInfo) []*RegionInfo) error {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return loadRegions(s.regionStorage, f)
	}
	return loadRegions(s.Base, f)
}

// SaveRegion saves one region to storage.
func (s *Storage) SaveRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return s.regionStorage.SaveRegion(region)
	}
	return saveProto(s.Base, regionPath(region.GetId()), region)
}

// DeleteRegion deletes one region from storage.
func (s *Storage) DeleteRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return deleteRegion(s.regionStorage, region)
	}
	return deleteRegion(s.Base, region)
}

// SaveConfig stores marshalable cfg to the configPath.
func (s *Storage) SaveConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errors.WithStack(err)
	}
	return s.Save(configPath, string(value))
}

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (s *Storage) LoadConfig(cfg interface{}) (bool, error) {
	value, err := s.Load(configPath)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

// SaveRules stores rule cfg to the rulesPath.
func (s *Storage) SaveRules(rules interface{}) error {
	value, err := json.Marshal(rules)
	if err != nil {
		return errors.WithStack(err)
	}
	return s.Save(rulesPath, string(value))
}

// LoadRules loads placement rules from storage.
func (s *Storage) LoadRules(rules interface{}) (bool, error) {
	value, err := s.Load(rulesPath)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), rules)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
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
	if s.regionStorage != nil {
		return s.regionStorage.FlushRegion()
	}
	return nil
}

// Close closes the s.
func (s *Storage) Close() error {
	if s.regionStorage != nil {
		return s.regionStorage.Close()
	}
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
