# Project1

## 目标

1. 实现standaloneKV
2. 实现raw_api，完成测试

## 具体实现

### standalone_stirage
需要实现的代码放在`kv/storage/standalone_storage/standalone_storage.go`，需要实现`StandAloneStorage`结构体，方法都已经定义好了，仅需要实现。这其实是一个简化的`raft_storage`，只要取其中一部分就行了，其余不懂的api参考`engine_util`就行了。

- 对于`StandAloneStorage`结构体来说，其实一个`engines`就足够了，调用一个包装好的`badger`数据库。
- `Start`方法，没有需要实现的内容
- `Stop`关闭数据库
- `Write`可以参考`engine_util.write_batch.go`，它其实就是把`batch`数组转化为一个`WriteBatch`，可以进行批处理写入数据库
- `Reader`可以参考`raft_storage`，需要一个`badger.txn`处理一致性，对于`Reader`来说需要返回一个对象实现`StorageReader`，这里新建一个对象文件`reader.go`可以参考`region_reader.go`。这里需要产生一个迭代器，我们这个迭代器比较简单直接使用`cf_iterator.go`就可以了。

### raw_api
需要实现的代码放在`kv/server/raw_api.go`，我们需要实现4个api，RawGet/ RawScan/ RawPut/ RawDelete
- RawGet, 比较简单，注意返回类型，空值需要额外标注
- RawPut，比较简单，利用`Storage.Modify`包装原有request中的（key,value）即可
- RawDelet，和RawPut类似
- RawScan，这个基于我们已经写的`reader.go`，特别注意，当得到迭代器的时候，需要`seek`到对应的`start_key`，不然找不到值。需要注意，limit的大小

## 测试方法

`make project1`

## 测试结果

```
➜ make project1                     
GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/server -run 1
=== RUN   TestRawGet1
--- PASS: TestRawGet1 (0.95s)
=== RUN   TestRawGetNotFound1
--- PASS: TestRawGetNotFound1 (0.89s)
=== RUN   TestRawPut1
--- PASS: TestRawPut1 (0.48s)
=== RUN   TestRawGetAfterRawPut1
--- PASS: TestRawGetAfterRawPut1 (0.55s)
=== RUN   TestRawGetAfterRawDelete1
--- PASS: TestRawGetAfterRawDelete1 (0.85s)
=== RUN   TestRawDelete1
--- PASS: TestRawDelete1 (0.60s)
=== RUN   TestRawScan1
--- PASS: TestRawScan1 (1.07s)
=== RUN   TestRawScanAfterRawPut1
--- PASS: TestRawScanAfterRawPut1 (0.87s)
=== RUN   TestRawScanAfterRawDelete1
--- PASS: TestRawScanAfterRawDelete1 (0.99s)
=== RUN   TestIterWithRawDelete1
--- PASS: TestIterWithRawDelete1 (0.89s)
PASS
ok      github.com/pingcap-incubator/tinykv/kv/server   8.147s
```