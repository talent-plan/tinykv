# Project2

## Project2a

code sturcture:

`tick()` trigger event -> request to `step()` -> `step()` handle event by calling `sendxxx` method that sending request to `Msgs` -> receivers handle request by calling `handlexxx` method and sending response to `Msgs` -> sender handle response by calling `handlexxxResponse`


### Project2aa

#### Target

1. leader election
2. pass Project2aa

#### Detail

roadmap from `raft/doc.go`

1. start from `raft.Raft.tick()`，advanced by `Node`。In this method, Each states have different behavior: `Follower` and `Candiate` will take election action, `Leader` will take heartbeat ation. There are two RPCs: `MessageType_MsgHup`, `MessageType_MsgBeat` mapping that two actions and needed to implement in `tickElection()`和`tickHeartbeat()`. And these will be handled by step.
2. Election, start from `startElection()`, the detail of this method descripted in paper, first it's state need transfer to `Follower`, it needs `lastLogIndex` and `LastLogTerm` implment in `RaftLog`。After that, it need send `sendRequestVote` to every peer without self. `ElectionTimeout` should be reset when receive heartbeat or timeout. Receiver revice handle this request and return response to sender. sender will count the tickets, if over half of peers are agree, it will become leader; if over half of peers are disagree, it will become follower
3. HeartBeat, start from `startHeartBeat()`,it differ from the paper: it divide heartbeat and appendEntries. So, this method is simple, just needed to send heartbeat periodically. 


### Project2ab

#### Target
1. Log replication
2. Pass the test project2ab

#### Detail 

1. When receiving `MessageType_ MsgPropose`, leader will call `appendEntry` to append entry and then call `bcastAppend` to send `MessageType_MsgAppend` to the remaining nodes.

2. When a node receives `MessageType_MsgAppend`, its status changes to follower. Refer to the raft paper for specific processing details. It should be noted that when processing this message, the follower needs to update the local commitIndex according to the leader's commitIndex, local commitIndex and the latest log index. However, the latest log index cannot be obtained by calling `follower.Getlastlogindex()`, but needs to be calculated by `MessageType_MsgAppend.Index` and the length of `MessageType_MsgAppend.Entry`. When the node processing is finished, send `MessageType_MsgAppendResponse` to the leader.

3. When the leader receives `MessageType_MsgAppendResponse`,it would update `raft.PRS` and `raft.Raftlog.Committed` according to the message. Refer to raft paper for details. When the leader finishes processing and finds that the commitindex of the `Msg.From` is inconsistent with the leader, continue to send the `MessageType_MsgAppend` until consistent. Similarly, when the leader's commitIndex is updated, the leader will send `MessageType_MsgAppend` to all other nodes.

4. When the leader sends heartbeat and receives`MessageType_MsgHeartbeatResponse` from other nodes. The leader will judge whether the commitIndex of this follower is consistent with the leader's commitIndex. If not, the leader will send `MessageType_MsgAppend` to the node.

### Project2ac

#### Target

1. impl `rawnode`. etc. `Ready()`,`HasReady()`,`Advance()`
2. pass project2ac

#### Detail

read `Ready` struct:

`Ready()`: return `Ready` struct, the parameters of that is fully explained.
`HasReady()`: If the `Ready()` needed to be called, it is decided by this method. There are some conditions: if `softSate` or `hardState`  equal to prev, if NO, this method return `true`. At the same time, if `CommittedEntries` or `Entries` are empty, if NO, this method return `true`. other return `false`
`Advance(rd Ready)`: it accept a Ready struct, and deal with it. In this test, there are only two sub-tests. The two need to consider are `r.raft.stable` and `r.raft.applied`, they should be update in this method, if `CommittedEntries` or `Entries` are not emptyv

## Project2b

#### Target

1. impl`PeerStorage.SaveReadyState` and `PeerStorage.Append`
2. impl`proposeRaftCommand` and `HandleRaftReady`
3. pass project2b

#### Detail

In the design of tinkv, a `Store` can have multiple Raft nodes, namely `Peer`, and these `Peer` belong to different `Region`. For a single `Peer`, if the state of the node changes, that is, if `HasReady()` returns true, the node needs to persist the State, log, and snapshot, and then the node processes and responds to the Msg passed by the upper layer.

When persisting State, logs, and snapshots, raftDB and kvDB are created by the underlying `badger`. The specific storage details are shown in the table:

| Key            | KeyFormat                      | Value          | DB |
|:----           |:----                           |:----           |:---|
|raft_log_key    |0x01 0x02 region_id 0x01 log_idx|Entry           |raft|
|raft_state_key  |0x01 0x02 region_id 0x02        |RaftLocalState  |raft|
|apply_state_key |0x01 0x02 region_id 0x03        |RaftApplyState  |kv  |
|region_state_key|0x01 0x03 region_id 0x01        |RegionLocalState|kv  |

In this experiment, the log persistence is completed in the `PeerStorage.Append` method. When storing, we need to pay attention to the following points:
1. Store only the latest log that has not been stored
2. If the uncommitted log stored in raftDB will never be committed in the future, it needs to be deleted from raftDB
3. After log storage is completed, `PeerStorage.raftState` needs to be updated, because at this time the log index and term of `raftState` have changed

`PeerStorage.SaveReadyState` is a package for persistent State, logs and snapshots. The log persistence calls the `PeerStorage.Append` method. Before persisting the State, we need to update `PeerStorage.raftState` according to `raft.Ready`, and then write `PeerStorage.raftState` into raftDB. The persistence of the snapshot will be completed in subsequent experiments.

The request from the client is mainly handled by `peerMsgHandler`, which has two main functions: one is `HandleMsgs` and the other is `HandleRaftReady`.

`HandleMsgs` processes all messages received from raftCh. In this experiment, we only need to pay attention to `message.MsgTypeRaftCmd`, and send such messages to the raft cluster after being processed by the `proposeRaftCommand` method.

When implementing `proposeRaftCommand`, we need to pay attention to the following points:
1. Judge the validity of the message, if the key does not belong to the current raftRegion, `message.Callback` returns `ErrResp`
2. Serialize the data contained in the message
3. Record the ProposalIndex and Term of this message, as well as `message.Callback` in the form of `proposal` structure
4. Encapsulate the serialized data into a log by the `propose` function

After the message is processed, the Raft node will have some status updates, `HandleRaftReady` Prepare and perform corresponding operations from the Raft module, including:
1. If the state of the raft node changes, call `PeerStorage.SaveReadyState` for persistence
2. Forward the message to raft, and then process the committed log in raft and perform corresponding operations in the database according to the log type
3. Each log corresponds to a `proposal`. When processing the log, we need to delete all proposals before this log, because the log corresponding to these proposals will never commit
4. After the log is executed in the database, according to the operation type, callback the corresponding resp
