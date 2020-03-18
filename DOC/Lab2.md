## LAB2 RaftKV
Raft is a consensus algorithm that is designed to be easy to understand. You can read material about the Raft itself at the Raft site, an interactive visualization of the Raft, and other resources, including the extended Raft paper. 
In this lab you will implement a high available kv server based on raft,  which needs you not only just implement the Raft algorithm but also use it practically, and bring you more challenges like manage raft’s persisted state with `badger`, add flow control for snapshot message, etc.
The lab have 3 parts, including:
1. Implement the basic Raft algorithm
2. Build a fault tolerant KV server on top of Raft
3. Add the support of raftlog GC and snapshot 

### Part A
#### The Code
In this part you will implement the basic raft algorithm, the code you need to implement is under `raft/` and the proto file in `proto/proto/eraftpb.proto`. Inside `raft/`, there are some skeleton code and test cases waiting for you. Difference from 6.824,  the raft algorithm you're gonna implement here is `tick based`, and internally to the `raft` package time is represented by an abstract "tick", the upper application will call `RawNode.Tick()` at regular intervals to drive the election and heartbeat timeout
This part can be broken down into 3 steps, including:

1. Leader election
2. Log replication
3. Raw node interface

#### Implement the Raft algorithm
`raft.Raft` in `raft/raft.go` provides the core of the Raft algorithm including message handling, driving the logic clock, etc.
#####  Leader election
To implement leader election, you may want to start with `raft.Raft.tick()` which is used to advance the internal logical clock by a single tick and hence drive the election timeout or heartbeat timeout. If you need to send out a message just push it to `raft.Raft.msgs` and all messages the raft received will be pass to `raft.Raft.Step()`, which  is the entrance of message handling, now you can handle some messages like `MsgRequestVote`, `MsgHeartbeat`  and their response. And also implement functions like `raft.Raft.becomeXXX` which used to update the raft internal state when the raft’s role changes.

##### Log replication
To implement log replication, you may want to start with handling `MsgAppend` and `MsgAppendResponse` on both sender and receiver side. Checkout `raft.RaftLog` in `raft/log.go` which is a helper struct that help you manage the raft log, in here you also need to interact with the upper application by the `Storage` interface define in `raft/storage.go` to get the persisted data like log entries and snapshot.
#### Implement the raw node interface
`raft.RawNode` in `raft/rawnode.go` is the interface we interact with the upper application, `raft.RawNode` contains `raft.Raft` and provide some wrapper functions like `RawNode.Tick()`and `RawNode.Step()`, also `raft.RawNode` provide `RawNode.Propose()` to propose new raft log. Another important struct `Ready` is also defined here, in `raft.Raft` when we handling message or advances logical clock, some interaction with the upper allipcation like sending message, stabling and applying logs, etc, are not happen immediately, instead these interaction are encapsulated in `Ready` and return by `RawNode.Ready()` to the upper application, after handling the returned `Ready`, the upper application also need to calling some functions like `RawNode.Advance()` to update `raft.Raft`'s internal state.
The upper application may use raft like:
```
for {
  select {
  case <-s.Ticker:
    Node.Tick()
  default:
    if Node.HasReady() {
      rd := Node.Ready()
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      if !raft.IsEmptySnap(rd.Snapshot) {
        processSnapshot(rd.Snapshot)
      }
      for _, entry := range rd.CommittedEntries {
        process(entry)
      }
      s.Node.Advance(rd)
    }
}
```
Here are some hint on this part:
Add any state you need to `raft.Raft`, `raft.RaftLog`, `raft.RawNode` and message on `eraftpb.proto`
The tests assume that the first time start raft should have term 0
The tests assume that the new elected leader should append a noop entry on its term
Put common checks like the message's term on `raft.Step()` instead of  `raft.stepLeader()`, `raft.stepFollower()`, etc.
Handling messages for all roles like `RequestVote` on `raft.Step()` instead of  `raft.stepLeader()`, `raft.stepFollower()`, etc.
The log entries append are quite different between leader and non-leader, there are different sources, checking and handling, be careful with that.
Don’t forget the election timeout should be different between peers.
Some wrapper functions in `rawnode.go` can implement with `raft.Step(local message)`
When start a new raft, get the last stabled state from `Storage` to initialize `raft.Raft` and `raft.RaftLog`
