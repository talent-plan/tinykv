# Project3 MultiRaftKV

In project2, you have built a high available kv server based on Raft, good work! But not enough, such kv server is backed by a single raft group which is not unlimited scalable, and every write request will wait until committed and then write to badger one by one, which is a key requirement to ensure consistency, but also kill any concurrency.

![multiraft](imgs/multiraft.png)

In this project you will implement a multi raft based kv server with balance scheduler, which consist of multiple raft groups, each raft group is responsible for a single key range which is named region here, the layout will be looked like the above diagram. Requests to a single region are handled just like before, yet multiple regions can handle requests concurrently which improve performance but also bring some new challenges like balancing the request to each region, etc.

This project have 3 part, including:

1. Implement membership change and leadership change to Raft algorithm
2. Implement conf change and region split on raftstore
3. Introduce scheduler

## Part A

In this part you will implement membership change and leadership change to the basic raft algorithm, these features are required by the next two parts. Membership change, namely conf change, is used to add or remove peers to the raft group, which can change the quorum of the raft group, so be careful. Leadership change, namely leader transfer, is used to transfer the leadership to another peer, which is very useful for balance.

### The Code

The code you need to modify is all about `raft/raft.go` and `raft/rawnode.go`, also see `proto/proto/eraft.proto` for new messages you need to handle. And both conf change and leader transfer are triggered by the upper application, so you may want to start at `raft/rawnode.go`.

### Implement leader transfer

To implement leader transfer, let’s introduce two new message types: `MsgTransferLeader` and `MsgTimeoutNow`. To transfer leadership you need to first call `raft.Raft.Step` with `MsgTransferLeader` message on the current leader, and to ensure the success of transfer, the current leader should first check the qualification of the transferee (namely transfer target) like: is the transferee’s log up to date, etc. If the transferee is not qualified, the current leader can choose to abort the transfer or help the transferee, since abort is not helping, let’s choose to help the transferee. If the transferee’s log is not up to date, the current leader should send a `MsgAppend` message to the transferee and stop accepting new proposes in case we end up cycling. So if the transferee is qualified (or after the current leader’s help), the leader should send a `MsgTimeoutNow` message to the transferee immediately, and after received a `MsgTimeoutNow` message the transferee should start a new election immediately regardless of its election timeout, with a higher term and up to date log, the transferee have great chance to step down the current leader and become the new leader.

### Implement conf change

Conf change algorithm you will implement here is not the joint consensus algorithm mentioned in the extended Raft paper that can add and/or remove arbitrary peers at once, instead it can only add or remove peers one by one, which is more simple and easy to reason about. Moreover, conf change start at calling leader’s `raft.RawNode.ProposeConfChange` which will propose an entry with `pb.Entry.EntryType` set to `EntryConfChange` and `pb.Entry.Data` set to the input `pb.ConfChange`. When entries with type `EntryConfChange` are committed, you must apply it through `RawNode.ApplyConfChange` with the `pb.ConfChange` in the entry, only then you can add or remove peer to this raft node through `raft.Raft.addNode` and `raft.Raft.removeNode` according to the `pb.ConfChange`.

> Hints:
>
> - `MsgTransferLeader` message is local message that not come from network
> - You set the `Message.from` of the `MsgTransferLeader` message to the transferee (namely transfer target)
> - To start a new election immediately you can call `Raft.Step` with `MsgHup` message
> - Call `pb.ConfChange.Marshal` to get bytes represent of `pb.ConfChange` and put it to `pb.Entry.Data`

## Part B

As Raft module supported membership change and leadership change now, in this part you need to make TinyKV support these admin commands based on part A. As you can see in `proto/proto/raft_cmdpb.proto`, there are four types of admin commands:

- CompactLog (Already implemented in project 2 part C)
- TransferLeader
- ChangePeer
- Split

`TransferLeader` and `ChangePeer` are the commands based on the Raft support of leadership change and membership change. These will be used as the basic operator steps for the balance scheduler. `Split` splits one Region into two Regions, that’s the base for multi raft. You will implement them step by step.

### The Code

All the changes are based on the implementation of project2, so the code you need to modify is all about `kv/raftstore/peer_msg_handler.go` and `kv/raftstore/peer.go`.

### Propose transfer leader

This step is quite simple. As a raft command, `TransferLeader` will be proposed as a Raft entry. But `TransferLeader` actually is an action no need to replicate to other peers, so you just need to call the `TransferLeader()` method of `RawNode` instead of `Propose()` for `TransferLeader` command.

### Implement conf change in raftstore

The conf change have two different types, `AddNode` and `RemoveNode`. Just as its name implies, it adds a Peer or removes a Peer from the Region. To implement conf change, you should learn the terminology of `RegionEpoch` first. `RegionEpoch` is a part of the meta information of `metapb.Region`. When a Region adds or removes Peer or splits, the Region’s epoch has changed. RegionEpoch’s `conf_ver` increases during ConfChange while `version` increases during split. It will be used to guarantee the latest region information under network isolation that two leaders in one Region.

You need to make raftstore support handling conf change commands. The process would be:

1. Propose conf change admin command by `ProposeConfChange`
2. After the log is committed, change the `RegionLocalState`, including `RegionEpoch` and `Peers` in `Region`
3. Call `ApplyConfChange()` of `raft.RawNode`

> Hints:
>
> - For executing `AddNode`, the newly added Peer will be created by heartbeat from the leader, check `maybeCreatePeer()` of `storeWorker`. At that time, this Peer is uninitialized and any information of its Region is unknown to us, so we use 0 to initialize its `Log Term` and `Index`. Leader then will know this Follower has no data (there exists a Log gap from 0 to 5) and it will directly send a snapshot to this Follower.
> - For executing `RemoveNode`, you should call the `destroyPeer()` explicitly to stop the Raft module. The destroy logic is provided for you.
> - Do not forget to update the region state in `storeMeta` of `GlobalContext`
> - Test code schedules the command of one conf change multiple times until the conf change is applied, so you need to consider how to ignore the duplicate commands of same conf change.

### Implement split region in raftstore

![raft_group](imgs/keyspace.png)

To support multi-raft, the system performs data sharding and makes each Raft group store just a portion of data. Hash and Range are commonly used for data sharding. TinyKV uses Range and the main reason is that Range can better aggregate keys with the same prefix, which is convenient for operations like scan. Besides, Range outperforms in split than Hash. Usually, it only involves metadata modification and there is no need to move data around.

``` protobuf
message Region {
 uint64 id = 1;
 // Region key range [start_key, end_key).
 bytes start_key = 2;
 bytes end_key = 3;
 RegionEpoch region_epoch = 4;
 repeated Peer peers = 5
}
```

Let’s take a relook at Region definition, it includes two fields `start_key` and `end_key` to indicate the range of data which the Region is responsible for. So split is the key step to support multi-raft. At the beginning, there is only one Region with range [“”, “”). You can regard the key space as a loop, so [“”, “”) stands for the whole space. With the data written, the split checker will checks the region size every `cfg.SplitRegionCheckTickInterval`, and generates a split key if possible to cut the Region into two parts, you can check the logic in
`kv/raftstore/runner/split_check.go`. The split key will be wrapped as a `MsgSplitRegion` handled by `onPrepareSplitRegion()`.

To make sure the ids of the newly created Region and Peers are unique, the ids are allocated by scheduler. It’s also provided, so you don’t have to implement it.
`onPrepareSplitRegion()` actually schedules a task for the pd worker to ask the scheduler for the ids. And make a split admin command after receiving the response from scheduler, see `onAskBatchSplit()` in `kv/raftstore/runner/pd_task.go`.

So your task is to implement the process of handling split admin command, just like conf change does. The provided framework supports multiple raft, see `kv/raftstore/router.go`. When a Region splits into two Regions, one of the Regions will inherit the metadata before splitting and just modify its Range and RegionEpoch while the other will create relevant meta information.

> Hints:
>
> - The corresponding Peer of this newly-created Region should be created by
`createPeer()` and registered to the router.regions. And the region’s info should be inserted into `regionRanges` in ctx.StoreMeta.
> - For the case region split with network isolation, the snapshot to be applied may have overlap with the existing region’s range. The check logic is in `checkSnapshot()` in `kv/raftstore/peer_msg_handler.go`. Please keep it in mind when implementing and take care of that case.
> - Use `engine_util.ExceedEndKey()` to compare with region’s end key. Because when the end key equals “”, any key will equal or greater than “”. > - There are more errors need to be considered: `ErrRegionNotFound`,
`ErrKeyNotInRegion`, `ErrEpochNotMatch`.
