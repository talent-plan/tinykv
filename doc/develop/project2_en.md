# Project2

code sturcture:

`tick()` trigger event -> request to `step()` -> `step()` handle event by calling `sendxxx` method that sending request to `Msgs` -> receivers handle request by calling `handlexxx` method and sending response to `Msgs` -> sender handle response by calling `handlexxxResponse`


## Project2aa

### Target

1. leader election
2. pass Project2aa

### Detail

roadmap from `raft/doc.go`

1. start from `raft.Raft.tick()`，advanced by `Node`。In this method, Each states have different behavior: `Follower` and `Candiate` will take election action, `Leader` will take heartbeat ation. There are two RPCs: `MessageType_MsgHup`, `MessageType_MsgBeat` mapping that two actions and needed to implement in `tickElection()`和`tickHeartbeat()`. And these will be handled by step.
2. Election, start from `startElection()`, the detail of this method descripted in paper, first it's state need transfer to `Follower`, it needs `lastLogIndex` and `LastLogTerm` implment in `RaftLog`。After that, it need send `sendRequestVote` to every peer without self. `ElectionTimeout` should be reset when receive heartbeat or timeout. Receiver revice handle this request and return response to sender. sender will count the tickets, if over half of peers are agree, it will become leader; if over half of peers are disagree, it will become follower
3. HeartBeat, start from `startHeartBeat()`,it differ from the paper: it divide heartbeat and appendEntries. So, this method is simple, just needed to send heartbeat periodically. 


### Result

```
➜ make project2aa
GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./raft -run 2AA
=== RUN   TestFollowerUpdateTermFromMessage2AA
--- PASS: TestFollowerUpdateTermFromMessage2AA (0.00s)
=== RUN   TestCandidateUpdateTermFromMessage2AA
--- PASS: TestCandidateUpdateTermFromMessage2AA (0.00s)
=== RUN   TestLeaderUpdateTermFromMessage2AA
--- PASS: TestLeaderUpdateTermFromMessage2AA (0.00s)
=== RUN   TestStartAsFollower2AA
--- PASS: TestStartAsFollower2AA (0.00s)
=== RUN   TestLeaderBcastBeat2AA
--- PASS: TestLeaderBcastBeat2AA (0.00s)
=== RUN   TestFollowerStartElection2AA
--- PASS: TestFollowerStartElection2AA (0.00s)
=== RUN   TestCandidateStartNewElection2AA
--- PASS: TestCandidateStartNewElection2AA (0.00s)
=== RUN   TestLeaderElectionInOneRoundRPC2AA
--- PASS: TestLeaderElectionInOneRoundRPC2AA (0.00s)
=== RUN   TestFollowerVote2AA
--- PASS: TestFollowerVote2AA (0.00s)
=== RUN   TestCandidateFallback2AA
--- PASS: TestCandidateFallback2AA (0.00s)
=== RUN   TestFollowerElectionTimeoutRandomized2AA
--- PASS: TestFollowerElectionTimeoutRandomized2AA (0.00s)
=== RUN   TestCandidateElectionTimeoutRandomized2AA
--- PASS: TestCandidateElectionTimeoutRandomized2AA (0.00s)
=== RUN   TestFollowersElectionTimeoutNonconflict2AA
--- PASS: TestFollowersElectionTimeoutNonconflict2AA (0.00s)
=== RUN   TestCandidatesElectionTimeoutNonconflict2AA
--- PASS: TestCandidatesElectionTimeoutNonconflict2AA (0.00s)
=== RUN   TestLeaderElection2AA
--- PASS: TestLeaderElection2AA (0.00s)
=== RUN   TestLeaderCycle2AA
--- PASS: TestLeaderCycle2AA (0.00s)
=== RUN   TestVoteFromAnyState2AA
--- PASS: TestVoteFromAnyState2AA (0.00s)
=== RUN   TestSingleNodeCandidate2AA
--- PASS: TestSingleNodeCandidate2AA (0.00s)
=== RUN   TestCandidateResetTermMessageType_MsgHeartbeat2AA
--- PASS: TestCandidateResetTermMessageType_MsgHeartbeat2AA (0.00s)
=== RUN   TestCandidateResetTermMessageType_MsgAppend2AA
--- PASS: TestCandidateResetTermMessageType_MsgAppend2AA (0.00s)
=== RUN   TestDisruptiveFollower2AA
--- PASS: TestDisruptiveFollower2AA (0.00s)
=== RUN   TestRecvMessageType_MsgBeat2AA
--- PASS: TestRecvMessageType_MsgBeat2AA (0.00s)
=== RUN   TestCampaignWhileLeader2AA
--- PASS: TestCampaignWhileLeader2AA (0.00s)
=== RUN   TestSplitVote2AA
--- PASS: TestSplitVote2AA (0.00s)
PASS
ok      github.com/pingcap-incubator/tinykv/raft        0.010s
```
## Project2ab

### Target
1. Log replication
2. Pass the test project2ab

### Detail 

1. When receiving `MessageType_ MsgPropose`, leader will call `appendEntry` to append entry and then call `bcastAppend` to send `MessageType_MsgAppend` to the remaining nodes.

2. When a node receives `MessageType_MsgAppend`, its status changes to follower. Refer to the raft paper for specific processing details. It should be noted that when processing this message, the follower needs to update the local commitIndex according to the leader's commitIndex, local commitIndex and the latest log index. However, the latest log index cannot be obtained by calling `follower.Getlastlogindex()`, but needs to be calculated by `MessageType_MsgAppend.Index` and the length of `MessageType_MsgAppend.Entry`. When the node processing is finished, send `MessageType_MsgAppendResponse` to the leader.

3. When the leader receives `MessageType_MsgAppendResponse`,it would update `raft.PRS` and `raft.Raftlog.Committed` according to the message. Refer to raft paper for details. When the leader finishes processing and finds that the commitindex of the `Msg.From` is inconsistent with the leader, continue to send the `MessageType_MsgAppend` until consistent. Similarly, when the leader's commitIndex is updated, the leader will send `MessageType_MsgAppend` to all other nodes.

4. When the leader sends heartbeat and receives`MessageType_MsgHeartbeatResponse` from other nodes. The leader will judge whether the commitIndex of this follower is consistent with the leader's commitIndex. If not, the leader will send `MessageType_MsgAppend` to the node.


### Result

```
make project2ab
GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./raft -run 2AB
=== RUN   TestLeaderStartReplication2AB
--- PASS: TestLeaderStartReplication2AB (0.00s)
=== RUN   TestLeaderCommitEntry2AB
--- PASS: TestLeaderCommitEntry2AB (0.00s)
=== RUN   TestLeaderAcknowledgeCommit2AB
--- PASS: TestLeaderAcknowledgeCommit2AB (0.00s)
=== RUN   TestLeaderCommitPrecedingEntries2AB
--- PASS: TestLeaderCommitPrecedingEntries2AB (0.00s)
=== RUN   TestFollowerCommitEntry2AB
--- PASS: TestFollowerCommitEntry2AB (0.00s)
=== RUN   TestFollowerCheckMessageType_MsgAppend2AB
--- PASS: TestFollowerCheckMessageType_MsgAppend2AB (0.00s)
=== RUN   TestFollowerAppendEntries2AB
--- PASS: TestFollowerAppendEntries2AB (0.00s)
=== RUN   TestLeaderSyncFollowerLog2AB
--- PASS: TestLeaderSyncFollowerLog2AB (0.00s)
=== RUN   TestVoteRequest2AB
--- PASS: TestVoteRequest2AB (0.00s)
=== RUN   TestVoter2AB
--- PASS: TestVoter2AB (0.00s)
=== RUN   TestLeaderOnlyCommitsLogFromCurrentTerm2AB
--- PASS: TestLeaderOnlyCommitsLogFromCurrentTerm2AB (0.00s)
=== RUN   TestProgressLeader2AB
--- PASS: TestProgressLeader2AB (0.00s)
=== RUN   TestLeaderElectionOverwriteNewerLogs2AB
--- PASS: TestLeaderElectionOverwriteNewerLogs2AB (0.00s)
=== RUN   TestLogReplication2AB
--- PASS: TestLogReplication2AB (0.00s)
=== RUN   TestSingleNodeCommit2AB
--- PASS: TestSingleNodeCommit2AB (0.00s)
=== RUN   TestCommitWithoutNewTermEntry2AB
--- PASS: TestCommitWithoutNewTermEntry2AB (0.00s)
=== RUN   TestCommitWithHeartbeat2AB
--- PASS: TestCommitWithHeartbeat2AB (0.00s)
=== RUN   TestDuelingCandidates2AB
--- PASS: TestDuelingCandidates2AB (0.00s)
=== RUN   TestCandidateConcede2AB
--- PASS: TestCandidateConcede2AB (0.00s)
=== RUN   TestOldMessages2AB
--- PASS: TestOldMessages2AB (0.00s)
=== RUN   TestProposal2AB
--- PASS: TestProposal2AB (0.00s)
=== RUN   TestHandleMessageType_MsgAppend2AB
--- PASS: TestHandleMessageType_MsgAppend2AB (0.00s)
=== RUN   TestRecvMessageType_MsgRequestVote2AB
--- PASS: TestRecvMessageType_MsgRequestVote2AB (0.00s)
=== RUN   TestAllServerStepdown2AB
--- PASS: TestAllServerStepdown2AB (0.00s)
=== RUN   TestHeartbeatUpdateCommit2AB
--- PASS: TestHeartbeatUpdateCommit2AB (0.00s)
=== RUN   TestLeaderIncreaseNext2AB
--- PASS: TestLeaderIncreaseNext2AB (0.00s)
PASS
ok      github.com/pingcap-incubator/tinykv/raft        0.005s
```

## Project2ac


### Target

1. impl `rawnode`. etc. `Ready()`,`HasReady()`,`Advance()`
2. pass project2ac

### Detail

read `Ready` struct:

`Ready()`: return `Ready` struct, the parameters of that is fully explained.
`HasReady()`: If the `Ready()` needed to be called, it is decided by this method. There are some conditions: if `softSate` or `hardState`  equal to prev, if NO, this method return `true`. At the same time, if `CommittedEntries` or `Entries` are empty, if NO, this method return `true`. other return `false`
`Advance(rd Ready)`: it accept a Ready struct, and deal with it. In this test, there are only two sub-tests. The two need to consider are `r.raft.stable` and `r.raft.applied`, they should be update in this method, if `CommittedEntries` or `Entries` are not empty

### Result
```
➜ make project2ac
GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./raft -run 2AC
=== RUN   TestRawNodeStart2AC
--- PASS: TestRawNodeStart2AC (0.00s)
=== RUN   TestRawNodeRestart2AC
--- PASS: TestRawNodeRestart2AC (0.00s)
PASS
ok      github.com/pingcap-incubator/tinykv/raft        0.002s
```

---

**final result**

```
➜ make project2a 
GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./raft -run 2A
=== RUN   TestFollowerUpdateTermFromMessage2AA
--- PASS: TestFollowerUpdateTermFromMessage2AA (0.00s)
=== RUN   TestCandidateUpdateTermFromMessage2AA
--- PASS: TestCandidateUpdateTermFromMessage2AA (0.00s)
=== RUN   TestLeaderUpdateTermFromMessage2AA
--- PASS: TestLeaderUpdateTermFromMessage2AA (0.00s)
=== RUN   TestStartAsFollower2AA
--- PASS: TestStartAsFollower2AA (0.00s)
=== RUN   TestLeaderBcastBeat2AA
--- PASS: TestLeaderBcastBeat2AA (0.00s)
=== RUN   TestFollowerStartElection2AA
--- PASS: TestFollowerStartElection2AA (0.00s)
=== RUN   TestCandidateStartNewElection2AA
--- PASS: TestCandidateStartNewElection2AA (0.00s)
=== RUN   TestLeaderElectionInOneRoundRPC2AA
--- PASS: TestLeaderElectionInOneRoundRPC2AA (0.00s)
=== RUN   TestFollowerVote2AA
--- PASS: TestFollowerVote2AA (0.00s)
=== RUN   TestCandidateFallback2AA
--- PASS: TestCandidateFallback2AA (0.00s)
=== RUN   TestFollowerElectionTimeoutRandomized2AA
--- PASS: TestFollowerElectionTimeoutRandomized2AA (0.00s)
=== RUN   TestCandidateElectionTimeoutRandomized2AA
--- PASS: TestCandidateElectionTimeoutRandomized2AA (0.00s)
=== RUN   TestFollowersElectionTimeoutNonconflict2AA
--- PASS: TestFollowersElectionTimeoutNonconflict2AA (0.00s)
=== RUN   TestCandidatesElectionTimeoutNonconflict2AA
--- PASS: TestCandidatesElectionTimeoutNonconflict2AA (0.00s)
=== RUN   TestLeaderStartReplication2AB
--- PASS: TestLeaderStartReplication2AB (0.00s)
=== RUN   TestLeaderCommitEntry2AB
--- PASS: TestLeaderCommitEntry2AB (0.00s)
=== RUN   TestLeaderAcknowledgeCommit2AB
--- PASS: TestLeaderAcknowledgeCommit2AB (0.00s)
=== RUN   TestLeaderCommitPrecedingEntries2AB
--- PASS: TestLeaderCommitPrecedingEntries2AB (0.00s)
=== RUN   TestFollowerCommitEntry2AB
--- PASS: TestFollowerCommitEntry2AB (0.00s)
=== RUN   TestFollowerCheckMessageType_MsgAppend2AB
--- PASS: TestFollowerCheckMessageType_MsgAppend2AB (0.00s)
=== RUN   TestFollowerAppendEntries2AB
--- PASS: TestFollowerAppendEntries2AB (0.00s)
=== RUN   TestLeaderSyncFollowerLog2AB
--- PASS: TestLeaderSyncFollowerLog2AB (0.00s)
=== RUN   TestVoteRequest2AB
--- PASS: TestVoteRequest2AB (0.00s)
=== RUN   TestVoter2AB
--- PASS: TestVoter2AB (0.00s)
=== RUN   TestLeaderOnlyCommitsLogFromCurrentTerm2AB
--- PASS: TestLeaderOnlyCommitsLogFromCurrentTerm2AB (0.00s)
=== RUN   TestProgressLeader2AB
--- PASS: TestProgressLeader2AB (0.00s)
=== RUN   TestLeaderElection2AA
--- PASS: TestLeaderElection2AA (0.00s)
=== RUN   TestLeaderCycle2AA
--- PASS: TestLeaderCycle2AA (0.00s)
=== RUN   TestLeaderElectionOverwriteNewerLogs2AB
--- PASS: TestLeaderElectionOverwriteNewerLogs2AB (0.00s)
=== RUN   TestVoteFromAnyState2AA
--- PASS: TestVoteFromAnyState2AA (0.00s)
=== RUN   TestLogReplication2AB
--- PASS: TestLogReplication2AB (0.00s)
=== RUN   TestSingleNodeCommit2AB
--- PASS: TestSingleNodeCommit2AB (0.00s)
=== RUN   TestCommitWithoutNewTermEntry2AB
--- PASS: TestCommitWithoutNewTermEntry2AB (0.00s)
=== RUN   TestCommitWithHeartbeat2AB
--- PASS: TestCommitWithHeartbeat2AB (0.00s)
=== RUN   TestDuelingCandidates2AB
--- PASS: TestDuelingCandidates2AB (0.00s)
=== RUN   TestCandidateConcede2AB
--- PASS: TestCandidateConcede2AB (0.00s)
=== RUN   TestSingleNodeCandidate2AA
--- PASS: TestSingleNodeCandidate2AA (0.00s)
=== RUN   TestOldMessages2AB
--- PASS: TestOldMessages2AB (0.00s)
=== RUN   TestProposal2AB
--- PASS: TestProposal2AB (0.00s)
=== RUN   TestHandleMessageType_MsgAppend2AB
--- PASS: TestHandleMessageType_MsgAppend2AB (0.00s)
=== RUN   TestRecvMessageType_MsgRequestVote2AB
--- PASS: TestRecvMessageType_MsgRequestVote2AB (0.00s)
=== RUN   TestAllServerStepdown2AB
--- PASS: TestAllServerStepdown2AB (0.00s)
=== RUN   TestCandidateResetTermMessageType_MsgHeartbeat2AA
--- PASS: TestCandidateResetTermMessageType_MsgHeartbeat2AA (0.00s)
=== RUN   TestCandidateResetTermMessageType_MsgAppend2AA
--- PASS: TestCandidateResetTermMessageType_MsgAppend2AA (0.00s)
=== RUN   TestDisruptiveFollower2AA
--- PASS: TestDisruptiveFollower2AA (0.00s)
=== RUN   TestHeartbeatUpdateCommit2AB
--- PASS: TestHeartbeatUpdateCommit2AB (0.00s)
=== RUN   TestRecvMessageType_MsgBeat2AA
--- PASS: TestRecvMessageType_MsgBeat2AA (0.00s)
=== RUN   TestLeaderIncreaseNext2AB
--- PASS: TestLeaderIncreaseNext2AB (0.00s)
=== RUN   TestCampaignWhileLeader2AA
--- PASS: TestCampaignWhileLeader2AA (0.00s)
=== RUN   TestSplitVote2AA
--- PASS: TestSplitVote2AA (0.00s)
=== RUN   TestRawNodeStart2AC
--- PASS: TestRawNodeStart2AC (0.00s)
=== RUN   TestRawNodeRestart2AC
--- PASS: TestRawNodeRestart2AC (0.00s)
PASS
ok      github.com/pingcap-incubator/tinykv/raft        0.012s
```
