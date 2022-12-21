// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
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

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package pegasus

import (
	"context"
	"encoding/json"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/XiaoMi/pegasus-go-client/pegasus2"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

var (
	RequestTimeout = 3 * time.Second
)

type pegasusDB struct {
	client   *pegasus2.Client
	sessions []pegasus.TableConnector
}

func (db *pegasusDB) InitThread(ctx context.Context, threadId int, _ int) context.Context {
	return context.WithValue(ctx, "tid", threadId)
}

func (db *pegasusDB) CleanupThread(_ context.Context) {
}

func (db *pegasusDB) Close() error {
	for _, s := range db.sessions {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return db.client.Close()
}

func (db *pegasusDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	timeoutCtx, _ := context.WithTimeout(ctx, RequestTimeout)
	s := db.sessions[ctx.Value("tid").(int)]

	rawValue, err := s.Get(timeoutCtx, []byte(key), []byte(""))
	if err == nil {
		var value map[string][]byte
		json.Unmarshal(rawValue, value)

		result := make(map[string][]byte)
		for _, field := range fields {
			if v, ok := value[field]; ok {
				result[field] = v
			}
		}

		return result, nil
	} else {
		pegalog.GetLogger().Println(err)
		return nil, err
	}
}

func (db *pegasusDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

func (db *pegasusDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	timeoutCtx, _ := context.WithTimeout(ctx, RequestTimeout)
	s := db.sessions[ctx.Value("tid").(int)]

	value, _ := json.Marshal(values)
	err := s.Set(timeoutCtx, []byte(key), nil, value)
	if err != nil {
		pegalog.GetLogger().Println(err)
	}
	return err
}

func (db *pegasusDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	timeoutCtx, _ := context.WithTimeout(ctx, RequestTimeout)
	s := db.sessions[ctx.Value("tid").(int)]

	value, _ := json.Marshal(values)
	err := s.Set(timeoutCtx, []byte(key), []byte(""), value)
	if err != nil {
		pegalog.GetLogger().Println(err)
	}
	return err
}

func (db *pegasusDB) Delete(ctx context.Context, table string, key string) error {
	timeoutCtx, _ := context.WithTimeout(ctx, RequestTimeout)
	s := db.sessions[ctx.Value("tid").(int)]

	err := s.Del(timeoutCtx, []byte(key), []byte(""))
	if err != nil {
		pegalog.GetLogger().Println(err)
	}
	return err
}

type pegasusCreator struct{}

func (pegasusCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	conf := p.MustGetString("meta_servers")
	metaServers := strings.Split(conf, ",")
	tbName := p.MustGetString("table")
	threadCount := p.MustGetInt("threadcount")

	cfg := pegasus.Config{MetaServers: metaServers}
	db := &pegasusDB{}
	db.sessions = make([]pegasus.TableConnector, threadCount)
	c := pegasus2.NewClient(cfg)
	for i := 0; i < threadCount; i++ {

		var err error
		timeoutCtx, _ := context.WithTimeout(context.Background(), RequestTimeout)
		tb, err := c.OpenTable(timeoutCtx, tbName)
		if err != nil {
			pegalog.GetLogger().Println("failed to open table: ", err)
			return nil, err
		}
		db.sessions[i] = tb
	}
	db.client = c
	return db, nil
}

func init() {
	ycsb.RegisterDBCreator("pegasus", pegasusCreator{})
}
