# Project 1: KV Separation

KV Separation is inspired by the paper WiscKey published in the FAST Conference, whose main idea is to separate the keys from values to minimize the I/O amplification. It is the first project you need to implement.
![kv separation](http://catkang.github.io/assets/img/lsm_upon_ssd/kv_split.png)

## Features
* WiscKey writes faster than LevelDB because the KV separation reduces write magnification and makes the compaction much more efficient, especially when KV size is large.
* The read performance of WiscKey may be a little poor than LevelDB, because KV Separation has the disadvantage of requiring one more operation to read value from disk. However, KV Separation allows the SST file to store more KV records, which greatly reduces the number of SST files. As a result, more KV records can be stored by the tablecache and blockcache. In conclusion, the read performance of KV Separation is not as bad as expected.
* The range operations become inefficient, because it changes from a sequential read to a sequential read plus multiple random reads. With the parallel IO capability of SSD, this loss can be offset as much as possible, which is due to the strong random access performance of SSD.

## Code Guidance
Here are some steps to implement this project. (You can also complete this project in your own way)

### Prerequisites
`leveldb`: the basic storage engine of TinyDB. Please learn the architecture of LSM-Tree by its wiki or readme text. In terms of code, you should at least know the implemetation of Read(function `DBImpl::Get` in `leveldb/db/db_impl.cc`), Write(function `DBImpl::Write` in `leveldb/db/db_impl.cc`) and Compaction (function `DBImpl::BackgroundCompaction` in `leveldb/db/db_impl.cc`). 

`rocksdb`: if you already have a good understanding of leveldb and plan to implement the project on rocksdb, please learn the implemetation of its Read(function `DBImpl::Get` in `rocksdb/db/db_impl/db_impl.cc`), Write(function `DBImpl::WriteImpl` in `rocksdb/db/db_impl/db_impl_write.cc`) and Compaction(function `DBImpl::BackgroundCompaction` in `rocksdb/db/db_impl/db_impl_compaction_flush.cc`).

### Initialization
First, you need to initialize a vLog file to store values. For convenience, the original log file structure of leveldb and rocksdb can be use for reference. Take leveldb as an example. The original log file is initialized when the database is opened in the function `DB::Open` (`leveldb/db/db_impl.cc`). Please implement a function to initialize a new vLog file.

### Write
Unlike LevelDB, the KV pairs are first written to the vLog file. The record written to the memtable and SST files is actually <key,address>. The address points to the offset of the KV pairs in the vLog file. Therefore, the function `DBImpl::Write` (`leveldb/db/db_impl.cc`) needs to be modified. First, write the value into the vLog file and get the address. Then write key, address and log_number into LSM tree.

### Read
The result read from a memtable or SST file is actually an address pointing to a location in the vLog file. As a result, the function `DBImpl::Get` (`leveldb/db/db_impl.cc`) alse needs to be modified. After getting the address and log_number from LSM tree, the real value needs to be read from the vLog file pointed from the address.

### Crash Consistency
In LevelDB's implementation, when immemtbale is flushed into the SST files successfully and the change is already logged in the manifest file (VersionSet::LogAndApply succeeds), the LOG file will be deleted. Otherwise, the database will replay the log file during recovery.

However, vLog file cannot be deleted in KV Separation, because the contents of each value are still stored here. So what to do when recovering from failure? It is impossible to replay the vLog file from beginning to end. WiscKey's approach is to add `HeadInfo:pos` to VersionEdit before immemtable being flushed. `HeadInfo:pos` means the checkpoint in vLog, which means that the KV record before `HeadInfo:pos` has been successfully written to the SST files. When recovering, `VersionSet::Recover` (`leveldb/db/version_set.cc`) will replay the edit record, get the HeadInfo. After getting the `HeadInfo:pos`, it will replay the vLog file from the pos location.

### Garbage Collection
Key-value stores based on standard LSM-trees do not immediately reclaim free space when a key-value pair is deleted or overwritten. During compaction, if data relating to a deleted or overwritten key-value pair is found, the data is discarded and space is reclaimed. In WiscKey, only invalid keys are reclaimed by the LSM tree compaction. Since WiscKey does not compact values, it needs a special garbage collector to reclaim free space in the vLog.

![gc](https://github.com/joker-qi/WiscKey/raw/master/images/garbage.png)

WiscKey targets a lightweight and online garbage collector. To make this possible, WiscKey introduces a new data layout as shown in the figure above: the tuple (key size, value size, key, value) is stored in the vLog.

During garbage collection, WiscKey first reads a chunk of key-value pairs (e.g., several MBs) from the tail of the vLog, then finds which of those values are valid (not yet overwritten or deleted) by querying the LSM-tree. WiscKey then appends valid values back to the head of the vLog. Finally, it frees the space occupied previously by the chunk, and updates the tail accordingly.

Please implement a garbage collection function which should be called when appropriate. For example, you can record `drop_count` which means the number of invalid keys during compactions (function `DBImpl::BackgroundCompaction` in `leveldb/db/db_impl.cc`). When `drop_count` reaches a certain threshold, garbage collection is triggered.

## Optimizations
The above implementation still has many shortcomings. The paper also mentions some optimizations you can implement. Moreover, it is better if you have other optimization ideas and implement them, which will get you a higher score.