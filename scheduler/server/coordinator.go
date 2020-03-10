// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/scheduler/pkg/logutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	runSchedulerCheckInterval = 3 * time.Second
	collectFactor             = 0.8
	collectTimeout            = 5 * time.Minute
	maxScheduleRetries        = 10
	maxLoadConfigRetries      = 10

	regionheartbeatSendChanCap = 1024

	patrolScanRegionLimit = 128 // It takes about 14 minutes to iterate 1 million regions.
)

var (
	errSchedulerExisted  = errors.New("scheduler existed")
	errSchedulerNotFound = errors.New("scheduler not found")
)

// coordinator is used to manage all schedulers and checkers to decide if the region needs to be scheduled.
type coordinator struct {
	sync.RWMutex

	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	cluster      *RaftCluster
	checkers     *schedule.CheckerController
	schedulers   map[string]*scheduleController
	opController *schedule.OperatorController
	hbStreams    *heartbeatStreams
}

// newCoordinator creates a new coordinator.
func newCoordinator(ctx context.Context, cluster *RaftCluster, hbStreams *heartbeatStreams) *coordinator {
	ctx, cancel := context.WithCancel(ctx)
	opController := schedule.NewOperatorController(ctx, cluster, hbStreams)
	return &coordinator{
		ctx:          ctx,
		cancel:       cancel,
		cluster:      cluster,
		checkers:     schedule.NewCheckerController(ctx, cluster, opController),
		schedulers:   make(map[string]*scheduleController),
		opController: opController,
		hbStreams:    hbStreams,
	}
}

// patrolRegions is used to scan regions.
// The checkers will check these regions to decide if they need to do some operations.
func (c *coordinator) patrolRegions() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	timer := time.NewTimer(c.cluster.GetPatrolRegionInterval())
	defer timer.Stop()

	log.Info("coordinator starts patrol regions")
	var key []byte
	for {
		select {
		case <-timer.C:
			timer.Reset(c.cluster.GetPatrolRegionInterval())
		case <-c.ctx.Done():
			log.Info("patrol regions has been stopped")
			return
		}

		regions := c.cluster.ScanRegions(key, nil, patrolScanRegionLimit)
		if len(regions) == 0 {
			// Resets the scan key.
			key = nil
			continue
		}

		for _, region := range regions {
			// Skips the region if there is already a pending operator.
			if c.opController.GetOperator(region.GetID()) != nil {
				continue
			}

			checkerIsBusy, ops := c.checkers.CheckRegion(region)
			if checkerIsBusy {
				break
			}

			key = region.GetEndKey()
			if ops != nil {
				c.opController.AddOperator(ops...)
			}
		}
	}
}

func (c *coordinator) run() {
	ticker := time.NewTicker(runSchedulerCheckInterval)
	defer ticker.Stop()
	log.Info("coordinator starts to collect cluster information")
	log.Info("coordinator starts to run schedulers")
	var (
		scheduleNames []string
		configs       []string
		err           error
	)
	for i := 0; i < maxLoadConfigRetries; i++ {
		scheduleNames, configs, err = c.cluster.storage.LoadAllScheduleConfig()
		if err == nil {
			break
		}
		log.Error("cannot load schedulers' config", zap.Int("retry-times", i), zap.Error(err))
	}
	if err != nil {
		log.Fatal("cannot load schedulers' config", zap.Error(err))
	}

	scheduleCfg := c.cluster.opt.Load().Clone()
	// The new way to create scheduler with the independent configuration.
	for i, name := range scheduleNames {
		data := configs[i]
		typ := schedule.FindSchedulerTypeByName(name)
		var cfg config.SchedulerConfig
		for _, c := range scheduleCfg.Schedulers {
			if c.Type == typ {
				cfg = c
				break
			}
		}
		if len(cfg.Type) == 0 {
			log.Error("the scheduler type not found", zap.String("scheduler-name", name))
			continue
		}
		if cfg.Disable {
			log.Info("skip create scheduler with independent configuration", zap.String("scheduler-name", name), zap.String("scheduler-type", cfg.Type))
			continue
		}
		s, err := schedule.CreateScheduler(cfg.Type, c.opController, c.cluster.storage, schedule.ConfigJSONDecoder([]byte(data)))
		if err != nil {
			log.Error("can not create scheduler with independent configuration", zap.String("scheduler-name", name), zap.Error(err))
			continue
		}
		log.Info("create scheduler with independent configuration", zap.String("scheduler-name", s.GetName()))
		if err = c.addScheduler(s); err != nil {
			log.Error("can not add scheduler with independent configuration", zap.String("scheduler-name", s.GetName()), zap.Error(err))
		}
	}

	// The old way to create the scheduler.
	k := 0
	for _, schedulerCfg := range scheduleCfg.Schedulers {
		if schedulerCfg.Disable {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
			log.Info("skip create scheduler", zap.String("scheduler-type", schedulerCfg.Type))
			continue
		}

		s, err := schedule.CreateScheduler(schedulerCfg.Type, c.opController, c.cluster.storage, schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args))
		if err != nil {
			log.Error("can not create scheduler", zap.String("scheduler-type", schedulerCfg.Type), zap.Error(err))
			continue
		}

		log.Info("create scheduler", zap.String("scheduler-name", s.GetName()))
		if err = c.addScheduler(s, schedulerCfg.Args...); err != nil && err != errSchedulerExisted {
			log.Error("can not add scheduler", zap.String("scheduler-name", s.GetName()), zap.Error(err))
		} else {
			// Only records the valid scheduler config.
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
		}
	}

	// Removes the invalid scheduler config and persist.
	scheduleCfg.Schedulers = scheduleCfg.Schedulers[:k]
	c.cluster.opt.Store(scheduleCfg)

	c.wg.Add(1)
	// Starts to patrol regions.
	go c.patrolRegions()
}

func (c *coordinator) stop() {
	c.cancel()
}

func (c *coordinator) getSchedulers() []string {
	c.RLock()
	defer c.RUnlock()

	names := make([]string, 0, len(c.schedulers))
	for name := range c.schedulers {
		names = append(names, name)
	}
	return names
}

func (c *coordinator) addScheduler(scheduler schedule.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errSchedulerExisted
	}

	s := newScheduleController(c, scheduler)
	if err := s.Prepare(c.cluster); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.GetName()] = s
	c.cluster.opt.AddSchedulerCfg(s.GetType(), args)

	return nil
}

func (c *coordinator) removeScheduler(name string) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return ErrNotBootstrapped
	}
	s, ok := c.schedulers[name]
	if !ok {
		return errSchedulerNotFound
	}

	s.Stop()
	delete(c.schedulers, name)

	var err error
	opt := c.cluster.opt
	if err = opt.RemoveSchedulerCfg(s.Ctx(), name); err != nil {
		log.Error("can not remove scheduler", zap.String("scheduler-name", name), zap.Error(err))
	} else {
		err = c.cluster.storage.RemoveScheduleConfig(name)
		if err != nil {
			log.Error("can not remove the scheduler config", zap.Error(err))
		}
	}
	return err
}

func (c *coordinator) runScheduler(s *scheduleController) {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer s.Cleanup(c.cluster)

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(s.GetInterval())
			if !s.AllowSchedule() {
				continue
			}
			if op := s.Schedule(); op != nil {
				c.opController.AddOperator(op)
			}

		case <-s.Ctx().Done():
			log.Info("scheduler has been stopped",
				zap.String("scheduler-name", s.GetName()),
				zap.Error(s.Ctx().Err()))
			return
		}
	}
}

// scheduleController is used to manage a scheduler to schedule.
type scheduleController struct {
	schedule.Scheduler
	cluster      *RaftCluster
	opController *schedule.OperatorController
	nextInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

// newScheduleController creates a new scheduleController.
func newScheduleController(c *coordinator, s schedule.Scheduler) *scheduleController {
	ctx, cancel := context.WithCancel(c.ctx)
	return &scheduleController{
		Scheduler:    s,
		cluster:      c.cluster,
		opController: c.opController,
		nextInterval: s.GetMinInterval(),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (s *scheduleController) Ctx() context.Context {
	return s.ctx
}

func (s *scheduleController) Stop() {
	s.cancel()
}

func (s *scheduleController) Schedule() *operator.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// If we have schedule, reset interval to the minimal interval.
		if op := s.Scheduler.Schedule(s.cluster); op != nil {
			s.nextInterval = s.Scheduler.GetMinInterval()
			return op
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}

// GetInterval returns the interval of scheduling for a scheduler.
func (s *scheduleController) GetInterval() time.Duration {
	return s.nextInterval
}

// AllowSchedule returns if a scheduler is allowed to schedule.
func (s *scheduleController) AllowSchedule() bool {
	return s.Scheduler.IsScheduleAllowed(s.cluster)
}
