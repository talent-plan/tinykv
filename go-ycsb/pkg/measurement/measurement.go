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

package measurement

import (
	"bufio"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

var header = []string{"Operation", "Takes(s)", "Count", "OPS", "Avg(us)", "Min(us)", "Max(us)", "99th(us)", "99.9th(us)", "99.99th(us)"}

type measurement struct {
	sync.RWMutex

	p *properties.Properties

	measurer ycsb.Measurer
}

func (m *measurement) measure(op string, start time.Time, lan time.Duration) {
	m.Lock()
	m.measurer.Measure(op, start, lan)
	m.Unlock()
}

func (m *measurement) output() {
	m.RLock()
	defer m.RUnlock()

	outFile := m.p.GetString(prop.MeasurementRawOutputFile, "")
	var w *bufio.Writer
	if outFile == "" {
		w = bufio.NewWriter(os.Stdout)
	} else {
		f, err := os.Create(outFile)
		if err != nil {
			panic("failed to create output file: " + err.Error())
		}
		defer f.Close()
		w = bufio.NewWriter(f)
	}

	err := globalMeasure.measurer.Output(w)
	if err != nil {
		panic("failed to write output: " + err.Error())
	}

	err = w.Flush()
	if err != nil {
		panic("failed to flush output: " + err.Error())
	}
}

func (m *measurement) summary() {
	m.RLock()
	globalMeasure.measurer.Summary()
	m.RUnlock()
}

// InitMeasure initializes the global measurement.
func InitMeasure(p *properties.Properties) {
	globalMeasure = new(measurement)
	globalMeasure.p = p
	measurementType := p.GetString(prop.MeasurementType, prop.MeasurementTypeDefault)
	switch measurementType {
	case "histogram":
		globalMeasure.measurer = InitHistograms(p)
	case "raw", "csv":
		globalMeasure.measurer = InitCSV()
	default:
		panic("unsupported measurement type: " + measurementType)
	}
	EnableWarmUp(p.GetInt64(prop.WarmUpTime, 0) > 0)
}

// Output prints the complete measurements.
func Output() {
	globalMeasure.output()
}

// Summary prints the measurement summary.
func Summary() {
	globalMeasure.summary()
}

// EnableWarmUp sets whether to enable warm-up.
func EnableWarmUp(b bool) {
	if b {
		atomic.StoreInt32(&warmUp, 1)
	} else {
		atomic.StoreInt32(&warmUp, 0)
	}
}

// IsWarmUpFinished returns whether warm-up is finished or not.
func IsWarmUpFinished() bool {
	return atomic.LoadInt32(&warmUp) == 0
}

// Measure measures the operation.
func Measure(op string, start time.Time, lan time.Duration) {
	if IsWarmUpFinished() {
		globalMeasure.measure(op, start, lan)
	}
}

var globalMeasure *measurement
var warmUp int32 // use as bool, 1 means in warmup progress, 0 means warmup finished.
