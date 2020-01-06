// Copyright 2018 PingCAP, Inc.
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

package schedulers

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/apiutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
)

func init() {

	// args: [start-key, end-key, range-name].
	schedule.RegisterSliceDecoderBuilder("scatter-range", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 3 {
				return errors.New("should specify the range and the name")
			}
			if len(args[2]) == 0 {
				return errors.New("the range name is invalid")
			}
			conf, ok := v.(*scatterRangeSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			conf.StartKey = args[0]
			conf.EndKey = args[1]
			conf.RangeName = args[2]
			return nil
		}
	})

	schedule.RegisterScheduler("scatter-range", func(opController *schedule.OperatorController, storage *core.Storage, decode schedule.ConfigDecoder) (schedule.Scheduler, error) {
		config := &scatterRangeSchedulerConfig{
			storage: storage,
		}
		decode(config)
		rangeName := config.RangeName
		if len(rangeName) == 0 {
			return nil, errors.New("the range name is invalid")
		}
		return newScatterRangeScheduler(opController, storage, config), nil
	})
}

const scatterRangeScheduleType = "scatter-range"

type scatterRangeSchedulerConfig struct {
	mu        sync.RWMutex
	storage   *core.Storage
	RangeName string `json:"range-name"`
	StartKey  string `json:"start-key"`
	EndKey    string `json:"end-key"`
}

func (conf *scatterRangeSchedulerConfig) BuildWithArgs(args []string) error {
	if len(args) != 3 {
		return errors.New("scatter range need 3 arguments to setup config")
	}
	conf.mu.Lock()
	defer conf.mu.Unlock()

	conf.RangeName = args[0]
	conf.StartKey = args[1]
	conf.EndKey = args[2]
	return nil
}

func (conf *scatterRangeSchedulerConfig) Clone() *scatterRangeSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &scatterRangeSchedulerConfig{
		StartKey:  conf.StartKey,
		EndKey:    conf.EndKey,
		RangeName: conf.RangeName,
	}
}

func (conf *scatterRangeSchedulerConfig) Persist() error {
	name := conf.getScheduleName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	conf.storage.SaveScheduleConfig(name, data)
	return nil
}

func (conf *scatterRangeSchedulerConfig) GetRangeName() string {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return conf.RangeName
}

func (conf *scatterRangeSchedulerConfig) GetStartKey() []byte {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return []byte(conf.StartKey)
}

func (conf *scatterRangeSchedulerConfig) GetEndKey() []byte {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return []byte(conf.EndKey)
}

func (conf *scatterRangeSchedulerConfig) getScheduleName() string {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return fmt.Sprintf("scatter-range-%s", conf.RangeName)
}

type scatterRangeScheduler struct {
	*baseScheduler
	name          string
	config        *scatterRangeSchedulerConfig
	balanceLeader schedule.Scheduler
	balanceRegion schedule.Scheduler
	handler       http.Handler
}

// newScatterRangeScheduler creates a scheduler that balances the distribution of leaders and regions that in the specified key range.
func newScatterRangeScheduler(opController *schedule.OperatorController, storage *core.Storage, config *scatterRangeSchedulerConfig) schedule.Scheduler {
	base := newBaseScheduler(opController)

	name := config.getScheduleName()
	handler := newScatterRangeHandler(config)
	scheduler := &scatterRangeScheduler{
		baseScheduler: base,
		config:        config,
		handler:       handler,
		name:          name,
		balanceLeader: newBalanceLeaderScheduler(
			opController,
			WithBalanceLeaderName("scatter-range-leader"),
			WithBalanceLeaderCounter(scatterRangeLeaderCounter),
		),
		balanceRegion: newBalanceRegionScheduler(
			opController,
			WithBalanceRegionName("scatter-range-region"),
			WithBalanceRegionCounter(scatterRangeRegionCounter),
		),
	}
	return scheduler
}

func (l *scatterRangeScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.handler.ServeHTTP(w, r)
}

func (l *scatterRangeScheduler) GetName() string {
	return l.name
}

func (l *scatterRangeScheduler) GetType() string {
	return scatterRangeScheduleType
}

func (l *scatterRangeScheduler) EncodeConfig() ([]byte, error) {
	l.config.mu.RLock()
	defer l.config.mu.RUnlock()
	return schedule.EncodeConfig(l.config)
}

func (l *scatterRangeScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return l.opController.OperatorCount(operator.OpRange) < cluster.GetRegionScheduleLimit()
}

func (l *scatterRangeScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	// isolate a new cluster according to the key range
	c := schedule.GenRangeCluster(cluster, []byte(l.config.GetStartKey()), []byte(l.config.GetEndKey()))
	c.SetTolerantSizeRatio(2)
	ops := l.balanceLeader.Schedule(c)
	if len(ops) > 0 {
		ops[0].SetDesc(fmt.Sprintf("scatter-range-leader-%s", l.config.RangeName))
		ops[0].AttachKind(operator.OpRange)
		schedulerCounter.WithLabelValues(l.GetName(), "new-leader-operator").Inc()
		return ops
	}
	ops = l.balanceRegion.Schedule(c)
	if len(ops) > 0 {
		ops[0].SetDesc(fmt.Sprintf("scatter-range-region-%s", l.config.RangeName))
		ops[0].AttachKind(operator.OpRange)
		schedulerCounter.WithLabelValues(l.GetName(), "new-region-operator").Inc()
		return ops
	}
	schedulerCounter.WithLabelValues(l.GetName(), "no-need").Inc()
	return nil
}

type scatterRangeHandler struct {
	scheduleName string
	storage      *core.Storage
	rd           *render.Render
	config       *scatterRangeSchedulerConfig
}

func (handler *scatterRangeHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	var args []string
	name, ok := input["range-name"].(string)
	if ok {
		if name != handler.config.GetRangeName() {
			handler.rd.JSON(w, http.StatusInternalServerError, errors.New("Cannot change the range name, please delete this schedule"))
			return
		}
		args = append(args, name)
	} else {
		args = append(args, handler.config.GetRangeName())
	}

	startKey, ok := input["start-key"].(string)
	if ok {
		args = append(args, startKey)
	} else {
		args = append(args, string(handler.config.GetStartKey()))
	}

	endKey, ok := input["end-key"].(string)
	if ok {
		args = append(args, endKey)
	} else {
		args = append(args, string(handler.config.GetEndKey()))
	}
	handler.config.BuildWithArgs(args)
	err := handler.config.Persist()
	if err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err)
	}
	handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *scatterRangeHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func newScatterRangeHandler(config *scatterRangeSchedulerConfig) http.Handler {
	h := &scatterRangeHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.UpdateConfig).Methods("POST")
	router.HandleFunc("/list", h.ListConfig).Methods("GET")
	return router
}
