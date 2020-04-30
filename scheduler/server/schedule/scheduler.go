// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	http.Handler
	GetName() string
	// GetType should in accordance with the name passing to schedule.RegisterScheduler()
	GetType() string
	EncodeConfig() ([]byte, error)
	GetMinInterval() time.Duration
	GetNextInterval(interval time.Duration) time.Duration
	Prepare(cluster opt.Cluster) error
	Cleanup(cluster opt.Cluster)
	Schedule(cluster opt.Cluster) *operator.Operator
	IsScheduleAllowed(cluster opt.Cluster) bool
}

// EncodeConfig encode the custom config for each scheduler.
func EncodeConfig(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// DecodeConfig decode the custom config for each scheduler.
func DecodeConfig(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// ConfigDecoder used to decode the config.
type ConfigDecoder func(v interface{}) error

// ConfigSliceDecoderBuilder used to build slice decoder of the config.
type ConfigSliceDecoderBuilder func([]string) ConfigDecoder

//ConfigJSONDecoder used to build a json decoder of the config.
func ConfigJSONDecoder(data []byte) ConfigDecoder {
	return func(v interface{}) error {
		return DecodeConfig(data, v)
	}
}

// ConfigSliceDecoder the default decode for the config.
func ConfigSliceDecoder(name string, args []string) ConfigDecoder {
	builder, ok := schedulerArgsToDecoder[name]
	if !ok {
		return func(v interface{}) error {
			return errors.Errorf("the config decoer do not register for %s", name)
		}
	}
	return builder(args)
}

// CreateSchedulerFunc is for creating scheduler.
type CreateSchedulerFunc func(opController *OperatorController, storage *core.Storage, dec ConfigDecoder) (Scheduler, error)

var schedulerMap = make(map[string]CreateSchedulerFunc)
var schedulerArgsToDecoder = make(map[string]ConfigSliceDecoderBuilder)

// RegisterScheduler binds a scheduler creator. It should be called in init()
// func of a package.
func RegisterScheduler(typ string, createFn CreateSchedulerFunc) {
	if _, ok := schedulerMap[typ]; ok {
		log.Fatal("duplicated scheduler", zap.String("type", typ))
	}
	schedulerMap[typ] = createFn
}

// RegisterSliceDecoderBuilder convert arguments to config. It should be called in init()
// func of package.
func RegisterSliceDecoderBuilder(typ string, builder ConfigSliceDecoderBuilder) {
	if _, ok := schedulerArgsToDecoder[typ]; ok {
		log.Fatal("duplicated scheduler", zap.String("type", typ))
	}
	schedulerArgsToDecoder[typ] = builder
}

// IsSchedulerRegistered check where the named scheduler type is registered.
func IsSchedulerRegistered(name string) bool {
	_, ok := schedulerMap[name]
	return ok
}

// CreateScheduler creates a scheduler with registered creator func.
func CreateScheduler(typ string, opController *OperatorController, storage *core.Storage, dec ConfigDecoder) (Scheduler, error) {
	fn, ok := schedulerMap[typ]
	if !ok {
		return nil, errors.Errorf("create func of %v is not registered", typ)
	}

	s, err := fn(opController, storage, dec)
	if err != nil {
		return nil, err
	}
	data, err := s.EncodeConfig()
	if err != nil {
		return nil, err
	}
	err = storage.SaveScheduleConfig(s.GetName(), data)
	return s, err
}

// FindSchedulerTypeByName finds the type of the specified name.
func FindSchedulerTypeByName(name string) string {
	var typ string
	for registerdType := range schedulerMap {
		if strings.Index(name, registerdType) != -1 {
			if len(registerdType) > len(typ) {
				typ = registerdType
			}
		}
	}
	return typ
}
