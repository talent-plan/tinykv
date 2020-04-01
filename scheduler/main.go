// Copyright 2016 PingCAP, Inc.
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

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap-incubator/tinykv/scheduler/pkg/logutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	// Register schedulers.
	_ "github.com/pingcap-incubator/tinykv/scheduler/server/schedulers"
)

func main() {
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])

	if cfg.Version {
		server.PrintPDInfo()
		exit(0)
	}

	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}

	if cfg.ConfigCheck {
		server.PrintConfigCheckMsg(cfg)
		exit(0)
	}

	// New zap logger
	err = cfg.SetupLogger()
	if err == nil {
		log.ReplaceGlobals(cfg.GetZapLogger(), cfg.GetZapLogProperties())
	} else {
		log.Fatal("initialize logger error", zap.Error(err))
	}
	// Flushing any buffered log entries
	defer log.Sync()

	// The old logger
	err = logutil.InitLogger(&cfg.Log)
	if err != nil {
		log.Fatal("initialize logger error", zap.Error(err))
	}

	server.LogPDInfo()

	for _, msg := range cfg.WarningMsgs {
		log.Warn(msg)
	}

	svr, err := server.CreateServer(cfg)
	if err != nil {
		log.Fatal("create server failed", zap.Error(err))
	}

	if err = server.InitHTTPClient(svr); err != nil {
		log.Fatal("initial http client for api handler failed", zap.Error(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())
	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := svr.Run(ctx); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Info("Got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
