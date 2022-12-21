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

package basic

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	simulateDelay         = "basicdb.simulatedelay"
	simulateDelayDefault  = int64(0)
	randomizeDelay        = "basicdb.randomizedelay"
	randomizeDelayDefault = true
)

type contextKey string

const stateKey = contextKey("basicDB")

type basicState struct {
	r *rand.Rand

	buf *bytes.Buffer
}

// BasicDB just prints out the requested operations, instead of doing them against a database
type basicDB struct {
	verbose        bool
	randomizeDelay bool
	toDelay        int64
}

func (db *basicDB) delay(ctx context.Context, state *basicState) {
	if db.toDelay == 0 {
		return
	}

	r := state.r
	delayTime := time.Duration(db.toDelay) * time.Millisecond
	if db.randomizeDelay {
		delayTime = time.Duration(r.Int63n(db.toDelay)) * time.Millisecond
		if delayTime == 0 {
			return
		}
	}

	select {
	case <-time.After(delayTime):
	case <-ctx.Done():
	}
}

func (db *basicDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	state := new(basicState)
	state.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	state.buf = new(bytes.Buffer)

	return context.WithValue(ctx, stateKey, state)
}

func (db *basicDB) CleanupThread(_ context.Context) {

}

func (db *basicDB) Close() error {
	return nil
}

func (db *basicDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	state := ctx.Value(stateKey).(*basicState)

	db.delay(ctx, state)

	if !db.verbose {
		return nil, nil
	}

	buf := state.buf
	s := fmt.Sprintf("READ %s %s [ ", table, key)
	buf.WriteString(s)

	if len(fields) > 0 {
		for _, f := range fields {
			buf.WriteString(f)
			buf.WriteByte(' ')
		}
	} else {
		buf.WriteString("<all fields> ")
	}
	buf.WriteByte(']')
	fmt.Println(buf.String())
	buf.Reset()
	return nil, nil
}

func (db *basicDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	panic("The basicDB has not implemented the batch operation")
}

func (db *basicDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	state := ctx.Value(stateKey).(*basicState)

	db.delay(ctx, state)

	if !db.verbose {
		return nil, nil
	}

	buf := state.buf
	s := fmt.Sprintf("SCAN %s %s %d [ ", table, startKey, count)
	buf.WriteString(s)

	if len(fields) > 0 {
		for _, f := range fields {
			buf.WriteString(f)
			buf.WriteByte(' ')
		}
	} else {
		buf.WriteString("<all fields> ")
	}
	buf.WriteByte(']')
	fmt.Println(buf.String())
	buf.Reset()
	return nil, nil
}

func (db *basicDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	state := ctx.Value(stateKey).(*basicState)

	db.delay(ctx, state)

	if !db.verbose {
		return nil
	}

	buf := state.buf
	s := fmt.Sprintf("UPDATE %s %s [ ", table, key)
	buf.WriteString(s)

	for key, value := range values {
		buf.WriteString(key)
		buf.WriteByte('=')
		buf.Write(value)
		buf.WriteByte(' ')
	}

	buf.WriteByte(']')
	fmt.Println(buf.String())
	buf.Reset()
	return nil
}

func (db *basicDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("The basicDB has not implemented the batch operation")
}

func (db *basicDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	state := ctx.Value(stateKey).(*basicState)

	db.delay(ctx, state)

	if !db.verbose {
		return nil
	}

	buf := state.buf
	insertRecord(buf, table, key, values)
	return nil
}

func (db *basicDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	state := ctx.Value(stateKey).(*basicState)

	db.delay(ctx, state)

	if !db.verbose {
		return nil
	}
	buf := state.buf
	for i, key := range keys {
		insertRecord(buf, table, key, values[i])
	}
	return nil
}

func insertRecord(buf *bytes.Buffer, table string, key string, values map[string][]byte) {
	s := fmt.Sprintf("INSERT %s %s [ ", table, key)
	buf.WriteString(s)
	for valueKey, value := range values {
		buf.WriteString(valueKey)
		buf.WriteByte('=')
		buf.Write(value)
		buf.WriteByte(' ')
	}
	buf.WriteByte(']')
	fmt.Println(buf.String())
	buf.Reset()
}

func (db *basicDB) Delete(ctx context.Context, table string, key string) error {
	state := ctx.Value(stateKey).(*basicState)

	db.delay(ctx, state)
	if !db.verbose {
		return nil
	}

	buf := state.buf
	s := fmt.Sprintf("DELETE %s %s", table, key)
	buf.WriteString(s)

	fmt.Println(buf.String())
	buf.Reset()
	return nil
}

func (db *basicDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	panic("The basicDB has not implemented the batch operation")
}

type basicDBCreator struct{}

func (basicDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	db := new(basicDB)

	db.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	db.randomizeDelay = p.GetBool(randomizeDelay, randomizeDelayDefault)
	db.toDelay = p.GetInt64(simulateDelay, simulateDelayDefault)

	return db, nil
}

func init() {
	fmt.Println("basic ok")
	ycsb.RegisterDBCreator("basic", basicDBCreator{})
}
