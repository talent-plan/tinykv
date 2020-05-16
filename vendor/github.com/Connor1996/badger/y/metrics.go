/*
 * Copyright (C) 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace  = "badger"
	labelPath  = "path"
	labelLevel = "target_level"
)

var (
	// LSMSize has size of the LSM in bytes
	LSMSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lsm_size",
	}, []string{labelPath})
	// VlogSize has size of the value log in bytes
	VlogSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "vlog_size",
	}, []string{labelPath})

	// These are cumulative

	// NumReads has cumulative number of reads
	NumReads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_reads",
	}, []string{labelPath})
	// NumWrites has cumulative number of writes
	NumWrites = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_writes",
	}, []string{labelPath})
	// NumBytesRead has cumulative number of bytes read
	NumBytesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_bytes_read",
	}, []string{labelPath})
	// NumVLogBytesWritten has cumulative number of bytes written
	NumVLogBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_bytes_written",
	}, []string{labelPath})
	// NumGets is number of gets
	NumGets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_gets",
	}, []string{labelPath})
	// NumPuts is number of puts
	NumPuts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_puts",
	}, []string{labelPath})
	// NumMemtableGets is number of memtable gets
	NumMemtableGets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_memtable_gets",
	}, []string{labelPath})

	// Level statistics

	// NumCompactionBytesWrite has cumulative size of keys read during compaction.
	NumCompactionBytesWrite = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_compaction_bytes_write",
	}, []string{labelPath, labelLevel})
	// NumCompactionBytesRead has cumulative size of keys write during compaction.
	NumCompactionBytesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_compaction_bytes_read",
	}, []string{labelPath, labelLevel})
	// NumCompactionBytesRead has cumulative size of discarded keys after compaction.
	NumCompactionBytesDiscard = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_compaction_bytes_discard",
	}, []string{labelPath, labelLevel})
	// NumCompactionKeysWrite has cumulative count of keys write during compaction.
	NumCompactionKeysWrite = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_compaction_keys_write",
	}, []string{labelPath, labelLevel})
	// NumCompactionKeysRead has cumulative count of keys read during compaction.
	NumCompactionKeysRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_compaction_keys_read",
	}, []string{labelPath, labelLevel})
	// NumCompactionKeysDiscard has cumulative count of discarded keys after compaction.
	NumCompactionKeysDiscard = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_compaction_keys_discard",
	}, []string{labelPath, labelLevel})
	// NumLSMGets is number of LMS gets
	NumLSMGets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_lsm_gets",
	}, []string{labelPath, labelLevel})
	// NumLSMBloomFalsePositive is number of LMS bloom hits
	NumLSMBloomFalsePositive = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_lsm_bloom_false_positive",
	}, []string{labelPath, labelLevel})

	// Histograms

	VlogSyncDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "vlog_sync_duration",
		Buckets:   prometheus.ExponentialBuckets(0.001, 1.5, 20),
	}, []string{labelPath})

	WriteLSMDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "write_lsm_duration",
		Buckets:   prometheus.ExponentialBuckets(0.0003, 1.5, 20),
	}, []string{labelPath})

	LSMGetDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "lsm_get_duration",
		Buckets:   prometheus.ExponentialBuckets(0.0003, 1.5, 20),
	}, []string{labelPath})

	LSMMultiGetDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "lsm_multi_get_duration",
		Buckets:   prometheus.ExponentialBuckets(0.0003, 1.5, 20),
	}, []string{labelPath})
)

type MetricsSet struct {
	path                string
	LSMSize             prometheus.Gauge
	VlogSize            prometheus.Gauge
	NumReads            prometheus.Counter
	NumWrites           prometheus.Counter
	NumBytesRead        prometheus.Counter
	NumVLogBytesWritten prometheus.Counter
	NumGets             prometheus.Counter
	NumPuts             prometheus.Counter
	NumMemtableGets     prometheus.Counter
	VlogSyncDuration    prometheus.Observer
	WriteLSMDuration    prometheus.Observer
	LSMGetDuration      prometheus.Observer
	LSMMultiGetDuration prometheus.Observer
}

func NewMetricSet(path string) *MetricsSet {
	return &MetricsSet{
		path:                path,
		LSMSize:             LSMSize.WithLabelValues(path),
		VlogSize:            VlogSize.WithLabelValues(path),
		NumReads:            NumReads.WithLabelValues(path),
		NumWrites:           NumWrites.WithLabelValues(path),
		NumBytesRead:        NumBytesRead.WithLabelValues(path),
		NumVLogBytesWritten: NumVLogBytesWritten.WithLabelValues(path),

		NumGets:             NumGets.WithLabelValues(path),
		NumPuts:             NumPuts.WithLabelValues(path),
		NumMemtableGets:     NumMemtableGets.WithLabelValues(path),
		VlogSyncDuration:    VlogSyncDuration.WithLabelValues(path),
		WriteLSMDuration:    WriteLSMDuration.WithLabelValues(path),
		LSMGetDuration:      LSMGetDuration.WithLabelValues(path),
		LSMMultiGetDuration: LSMMultiGetDuration.WithLabelValues(path),
	}
}

func (m *MetricsSet) NewLevelMetricsSet(levelLabel string) *LevelMetricsSet {
	return &LevelMetricsSet{
		MetricsSet:                m,
		NumLSMGets:                NumLSMGets.WithLabelValues(m.path, levelLabel),
		NumLSMBloomFalsePositive:  NumLSMBloomFalsePositive.WithLabelValues(m.path, levelLabel),
		NumCompactionKeysRead:     NumCompactionKeysRead.WithLabelValues(m.path, levelLabel),
		NumCompactionBytesRead:    NumCompactionBytesRead.WithLabelValues(m.path, levelLabel),
		NumCompactionKeysWrite:    NumCompactionKeysWrite.WithLabelValues(m.path, levelLabel),
		NumCompactionBytesWrite:   NumCompactionBytesWrite.WithLabelValues(m.path, levelLabel),
		NumCompactionKeysDiscard:  NumCompactionKeysDiscard.WithLabelValues(m.path, levelLabel),
		NumCompactionBytesDiscard: NumCompactionBytesDiscard.WithLabelValues(m.path, levelLabel),
	}
}

type LevelMetricsSet struct {
	*MetricsSet
	NumCompactionKeysRead     prometheus.Counter
	NumCompactionBytesRead    prometheus.Counter
	NumCompactionKeysWrite    prometheus.Counter
	NumCompactionBytesWrite   prometheus.Counter
	NumCompactionKeysDiscard  prometheus.Counter
	NumCompactionBytesDiscard prometheus.Counter
	NumLSMGets                prometheus.Counter
	NumLSMBloomFalsePositive  prometheus.Counter
}

type CompactionStats struct {
	KeysRead     int
	BytesRead    int
	KeysWrite    int
	BytesWrite   int
	KeysDiscard  int
	BytesDiscard int
}

func (m *LevelMetricsSet) UpdateCompactionStats(stats *CompactionStats) {
	m.NumCompactionKeysRead.Add(float64(stats.KeysRead))
	m.NumCompactionBytesRead.Add(float64(stats.BytesRead))

	m.NumCompactionKeysWrite.Add(float64(stats.KeysWrite))
	m.NumCompactionBytesWrite.Add(float64(stats.BytesWrite))

	m.NumCompactionKeysDiscard.Add(float64(stats.KeysDiscard))
	m.NumCompactionBytesDiscard.Add(float64(stats.BytesDiscard))
}

// These variables are global and have cumulative values for all kv stores.
func init() {
	prometheus.MustRegister(LSMSize)
	prometheus.MustRegister(VlogSize)
	prometheus.MustRegister(NumReads)
	prometheus.MustRegister(NumWrites)
	prometheus.MustRegister(NumBytesRead)
	prometheus.MustRegister(NumVLogBytesWritten)
	prometheus.MustRegister(NumLSMGets)
	prometheus.MustRegister(NumLSMBloomFalsePositive)
	prometheus.MustRegister(NumGets)
	prometheus.MustRegister(NumPuts)
	prometheus.MustRegister(NumMemtableGets)
	prometheus.MustRegister(VlogSyncDuration)
	prometheus.MustRegister(WriteLSMDuration)
	prometheus.MustRegister(LSMGetDuration)
	prometheus.MustRegister(LSMMultiGetDuration)
	prometheus.MustRegister(NumCompactionBytesWrite)
	prometheus.MustRegister(NumCompactionBytesRead)
	prometheus.MustRegister(NumCompactionBytesDiscard)
	prometheus.MustRegister(NumCompactionKeysRead)
	prometheus.MustRegister(NumCompactionKeysWrite)
	prometheus.MustRegister(NumCompactionKeysDiscard)
}
