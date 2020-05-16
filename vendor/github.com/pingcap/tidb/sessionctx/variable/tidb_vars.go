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

package variable

import (
	"os"
)

/*
	Steps to add a new TiDB specific system variable:

	1. Add a new variable name with comment in this file.
	2. Add the default value of the new variable in this file.
	3. Add SysVar instance in 'defaultSysVars' slice with the default value.
	4. Add a field in `SessionVars`.
	5. Update the `NewSessionVars` function to set the field to its default value.
	6. Update the `variable.SetSessionSystemVar` function to use the new value when SET statement is executed.
	7. If it is a global variable, add it in `session.loadCommonGlobalVarsSQL`.
	8. Update ValidateSetSystemVar if the variable's value need to be validated.
	9. Use this variable to control the behavior in code.
*/

// TiDB system variable names that only in session scope.
const (
	// tidb_snapshot is used for reading history data, the default value is empty string.
	// The value can be a datetime string like '2017-11-11 20:20:20' or a tso string. When this variable is set, the session reads history data of that time.
	TiDBSnapshot = "tidb_snapshot"

	// tidb_opt_agg_push_down is used to enable/disable the optimizer rule of aggregation push down.
	TiDBOptAggPushDown = "tidb_opt_agg_push_down"

	// tidb_opt_write_row_id is used to enable/disable the operations of insert、replace and update to _tidb_rowid.
	TiDBOptWriteRowID = "tidb_opt_write_row_id"

	// TiDBCurrentTS is used to get the current transaction timestamp.
	// It is read-only.
	TiDBCurrentTS = "tidb_current_ts"

	// tidb_config is a read-only variable that shows the config of the current server.
	TiDBConfig = "tidb_config"

	// tidb_general_log is used to log every query in the server in info level.
	TiDBGeneralLog = "tidb_general_log"

	// tidb_skip_isolation_level_check is used to control whether to return error when set unsupported transaction
	// isolation level.
	TiDBSkipIsolationLevelCheck = "tidb_skip_isolation_level_check"

	// TiDBReplicaRead is used for reading data from replicas, followers for example.
	TiDBReplicaRead = "tidb_replica_read"

	// TiDBAllowRemoveAutoInc indicates whether a user can drop the auto_increment column attribute or not.
	TiDBAllowRemoveAutoInc = "tidb_allow_remove_auto_inc"
)

// TiDB system variable names that both in session and global scope.
const (
	// tidb_build_stats_concurrency is used to speed up the ANALYZE statement, when a table has multiple indices,
	// those indices can be scanned concurrently, with the cost of higher system performance impact.
	TiDBBuildStatsConcurrency = "tidb_build_stats_concurrency"

	// tidb_distsql_scan_concurrency is used to set the concurrency of a distsql scan task.
	// A distsql scan task can be a table scan or a index scan, which may be distributed to many TiKV nodes.
	// Higher concurrency may reduce latency, but with the cost of higher memory usage and system performance impact.
	// If the query has a LIMIT clause, high concurrency makes the system do much more work than needed.
	TiDBDistSQLScanConcurrency = "tidb_distsql_scan_concurrency"

	// tidb_opt_insubquery_to_join_and_agg is used to enable/disable the optimizer rule of rewriting IN subquery.
	TiDBOptInSubqToJoinAndAgg = "tidb_opt_insubq_to_join_and_agg"

	// tidb_opt_correlation_threshold is a guard to enable row count estimation using column order correlation.
	TiDBOptCorrelationThreshold = "tidb_opt_correlation_threshold"

	// tidb_opt_correlation_exp_factor is an exponential factor to control heuristic approach when tidb_opt_correlation_threshold is not satisfied.
	TiDBOptCorrelationExpFactor = "tidb_opt_correlation_exp_factor"

	// tidb_opt_cpu_factor is the CPU cost of processing one expression for one row.
	TiDBOptCPUFactor = "tidb_opt_cpu_factor"
	// tidb_opt_copcpu_factor is the CPU cost of processing one expression for one row in coprocessor.
	TiDBOptCopCPUFactor = "tidb_opt_copcpu_factor"
	// tidb_opt_network_factor is the network cost of transferring 1 byte data.
	TiDBOptNetworkFactor = "tidb_opt_network_factor"
	// tidb_opt_scan_factor is the IO cost of scanning 1 byte data on TiKV.
	TiDBOptScanFactor = "tidb_opt_scan_factor"
	// tidb_opt_desc_factor is the IO cost of scanning 1 byte data on TiKV in desc order.
	TiDBOptDescScanFactor = "tidb_opt_desc_factor"
	// tidb_opt_seek_factor is the IO cost of seeking the start value in a range on TiKV.
	TiDBOptSeekFactor = "tidb_opt_seek_factor"
	// tidb_opt_memory_factor is the memory cost of storing one tuple.
	TiDBOptMemoryFactor = "tidb_opt_memory_factor"
	// tidb_opt_disk_factor is the IO cost of reading/writing one byte to temporary disk.
	TiDBOptDiskFactor = "tidb_opt_disk_factor"
	// tidb_opt_concurrency_factor is the CPU cost of additional one goroutine.
	TiDBOptConcurrencyFactor = "tidb_opt_concurrency_factor"

	// tidb_index_lookup_size is used for index lookup executor.
	// The index lookup executor first scan a batch of handles from a index, then use those handles to lookup the table
	// rows, this value controls how much of handles in a batch to do a lookup task.
	// Small value sends more RPCs to TiKV, consume more system resource.
	// Large value may do more work than needed if the query has a limit.
	TiDBIndexLookupSize = "tidb_index_lookup_size"

	// tidb_index_lookup_concurrency is used for index lookup executor.
	// A lookup task may have 'tidb_index_lookup_size' of handles at maximun, the handles may be distributed
	// in many TiKV nodes, we executes multiple concurrent index lookup tasks concurrently to reduce the time
	// waiting for a task to finish.
	// Set this value higher may reduce the latency but consumes more system resource.
	TiDBIndexLookupConcurrency = "tidb_index_lookup_concurrency"

	// tidb_index_lookup_join_concurrency is used for index lookup join executor.
	// IndexLookUpJoin starts "tidb_index_lookup_join_concurrency" inner workers
	// to fetch inner rows and join the matched (outer, inner) row pairs.
	TiDBIndexLookupJoinConcurrency = "tidb_index_lookup_join_concurrency"

	// tidb_index_serial_scan_concurrency is used for controlling the concurrency of index scan operation
	// when we need to keep the data output order the same as the order of index data.
	TiDBIndexSerialScanConcurrency = "tidb_index_serial_scan_concurrency"

	// TiDBMaxChunkSize is used to control the max chunk size during query execution.
	TiDBMaxChunkSize = "tidb_max_chunk_size"

	// TiDBInitChunkSize is used to control the init chunk size during query execution.
	TiDBInitChunkSize = "tidb_init_chunk_size"

	// tidb_enable_cascades_planner is used to control whether to enable the cascades planner.
	TiDBEnableCascadesPlanner = "tidb_enable_cascades_planner"

	// tidb_skip_utf8_check skips the UTF8 validate process, validate UTF8 has performance cost, if we can make sure
	// the input string values are valid, we can skip the check.
	TiDBSkipUTF8Check = "tidb_skip_utf8_check"

	// tidb_hash_join_concurrency is used for hash join executor.
	// The hash join outer executor starts multiple concurrent join workers to probe the hash table.
	TiDBHashJoinConcurrency = "tidb_hash_join_concurrency"

	// tidb_projection_concurrency is used for projection operator.
	// This variable controls the worker number of projection operator.
	TiDBProjectionConcurrency = "tidb_projection_concurrency"

	// tidb_hashagg_partial_concurrency is used for hash agg executor.
	// The hash agg executor starts multiple concurrent partial workers to do partial aggregate works.
	TiDBHashAggPartialConcurrency = "tidb_hashagg_partial_concurrency"

	// tidb_hashagg_final_concurrency is used for hash agg executor.
	// The hash agg executor starts multiple concurrent final workers to do final aggregate works.
	TiDBHashAggFinalConcurrency = "tidb_hashagg_final_concurrency"

	// tidb_backoff_lock_fast is used for tikv backoff base time in milliseconds.
	TiDBBackoffLockFast = "tidb_backoff_lock_fast"

	// tidb_backoff_weight is used to control the max back off time in TiDB.
	// The default maximum back off time is a small value.
	// BackOffWeight could multiply it to let the user adjust the maximum time for retrying.
	// Only positive integers can be accepted, which means that the maximum back off time can only grow.
	TiDBBackOffWeight = "tidb_backoff_weight"

	// tidb_ddl_reorg_worker_cnt defines the count of ddl reorg workers.
	TiDBDDLReorgWorkerCount = "tidb_ddl_reorg_worker_cnt"

	// tidb_ddl_reorg_batch_size defines the transaction batch size of ddl reorg workers.
	TiDBDDLReorgBatchSize = "tidb_ddl_reorg_batch_size"

	// tidb_ddl_error_count_limit defines the count of ddl error limit.
	TiDBDDLErrorCountLimit = "tidb_ddl_error_count_limit"

	// tidb_ddl_reorg_priority defines the operations priority of adding indices.
	// It can be: PRIORITY_LOW, PRIORITY_NORMAL, PRIORITY_HIGH
	TiDBDDLReorgPriority = "tidb_ddl_reorg_priority"

	// tidb_max_delta_schema_count defines the max length of deltaSchemaInfos.
	// deltaSchemaInfos is a queue that maintains the history of schema changes.
	TiDBMaxDeltaSchemaCount = "tidb_max_delta_schema_count"

	// tidb_scatter_region will scatter the regions for DDLs when it is ON.
	TiDBScatterRegion = "tidb_scatter_region"

	// TiDBWaitSplitRegionFinish defines the split region behaviour is sync or async.
	TiDBWaitSplitRegionFinish = "tidb_wait_split_region_finish"

	// TiDBWaitSplitRegionTimeout uses to set the split and scatter region back off time.
	TiDBWaitSplitRegionTimeout = "tidb_wait_split_region_timeout"

	// tidb_enable_radix_join indicates to use radix hash join algorithm to execute
	// HashJoin.
	TiDBEnableRadixJoin = "tidb_enable_radix_join"

	// tidb_constraint_check_in_place indicates to check the constraint when the SQL executing.
	// It could hurt the performance of bulking insert when it is ON.
	TiDBConstraintCheckInPlace = "tidb_constraint_check_in_place"

	// tidb_enable_vectorized_expression is used to control whether to enable the vectorized expression evaluation.
	TiDBEnableVectorizedExpression = "tidb_enable_vectorized_expression"

	// TIDBOptJoinReorderThreshold defines the threshold less than which
	// we'll choose a rather time consuming algorithm to calculate the join order.
	TiDBOptJoinReorderThreshold = "tidb_opt_join_reorder_threshold"

	// SlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
	TiDBSlowQueryFile = "tidb_slow_query_file"

	// TiDBEnableNoopFuncs set true will enable using fake funcs(like get_lock release_lock)
	TiDBEnableNoopFuncs = "tidb_enable_noop_functions"
)

// Default TiDB system variable values.
const (
	DefHostname                      = "localhost"
	DefIndexLookupConcurrency        = 4
	DefIndexLookupJoinConcurrency    = 4
	DefIndexSerialScanConcurrency    = 1
	DefIndexLookupSize               = 20000
	DefDistSQLScanConcurrency        = 15
	DefBuildStatsConcurrency         = 4
	DefSkipUTF8Check                 = false
	DefOptAggPushDown                = false
	DefOptWriteRowID                 = false
	DefOptCorrelationThreshold       = 0.9
	DefOptCorrelationExpFactor       = 1
	DefOptCPUFactor                  = 3.0
	DefOptCopCPUFactor               = 3.0
	DefOptNetworkFactor              = 1.0
	DefOptScanFactor                 = 1.5
	DefOptDescScanFactor             = 3.0
	DefOptSeekFactor                 = 20.0
	DefOptMemoryFactor               = 0.001
	DefOptDiskFactor                 = 1.5
	DefOptConcurrencyFactor          = 3.0
	DefOptInSubqToJoinAndAgg         = true
	DefCurretTS                      = 0
	DefInitChunkSize                 = 32
	DefMaxChunkSize                  = 1024
	DefMaxPreparedStmtCount          = -1
	DefWaitTimeout                   = 0
	DefTiDBGeneralLog                = 0
	DefTiDBRetryLimit                = 10
	DefTiDBDisableTxnAutoRetry       = true
	DefTiDBConstraintCheckInPlace    = false
	DefTiDBHashJoinConcurrency       = 5
	DefTiDBProjectionConcurrency     = 4
	DefTiDBDDLReorgWorkerCount       = 4
	DefTiDBDDLReorgBatchSize         = 256
	DefTiDBDDLErrorCountLimit        = 512
	DefTiDBMaxDeltaSchemaCount       = 1024
	DefTiDBHashAggPartialConcurrency = 4
	DefTiDBHashAggFinalConcurrency   = 4
	DefTiDBUseRadixJoin              = false
	DefEnableVectorizedExpression    = true
	DefTiDBOptJoinReorderThreshold   = 0
	DefTiDBSkipIsolationLevelCheck   = false
	DefTiDBScatterRegion             = false
	DefTiDBWaitSplitRegionFinish     = true
	DefWaitSplitRegionTimeout        = 300 // 300s
	DefTiDBEnableNoopFuncs           = false
	DefTiDBAllowRemoveAutoInc        = false
	DefInnodbLockWaitTimeout         = 50 // 50s
)

// Process global variables.
var (
	ProcessGeneralLog      uint32
	ddlReorgWorkerCounter  int32 = DefTiDBDDLReorgWorkerCount
	maxDDLReorgWorkerCount int32 = 128
	ddlReorgBatchSize      int32 = DefTiDBDDLReorgBatchSize
	ddlErrorCountlimit     int64 = DefTiDBDDLErrorCountLimit
	maxDeltaSchemaCount    int64 = DefTiDBMaxDeltaSchemaCount
	// Export for testing.
	MaxDDLReorgBatchSize  int32  = 10240
	MinDDLReorgBatchSize  int32  = 32
	ServerHostname, _            = os.Hostname()
	MaxOfMaxAllowedPacket uint64 = 1073741824
)
