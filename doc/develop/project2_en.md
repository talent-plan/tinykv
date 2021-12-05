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
