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

package ycsb

import (
	"io"
	"time"
)

// Measurer is used to capture measurements.
type Measurer interface {
	// Measure measures the latency of an operation.
	Measure(op string, start time.Time, latency time.Duration)

	// Summary writes a summary of the current measurement results to stdout.
	Summary()

	// Output writes the measurement results to the specified writer.
	Output(w io.Writer) error
}
