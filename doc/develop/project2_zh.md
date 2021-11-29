# Project2

实验中整体代码逻辑

tick 触发事件 -> 发送请求至step（不经过Msgs）-> step采取对应方法，调用sendxx方法发送request至Msgs（RPC) -> 接收端通过step处理request，调用handlexx方法，返回response至Msgs（RPC）-> 发送端通过step处理response，调用handlexx方法处理

## Project2aa

### 实验目的

1. 完成Raft的领导选举
2. 通过测试`project2aa`

### 实验思路

实验思路从`raft/doc.go`中获得。

1. 根据实验手册的简单提示，我们可以从`raft.Raft.tick()`，它是一个逻辑时钟，由Node推进，不过暂时2aa里用不到，所以我们只需要一开始实现`tick()`函数即可。不同state的raft，对应时钟有不同的行为：即`Follower`和`Candidate`只有选举的行为，而`Leader`只有心跳的行为，根据doc.go前两个rpc的提示(`MessageType_MsgHup`, `MessageType_MsgBeat`)实现`tickElection`和`tickHeartbeat`，这两个函数触发触发选举/心跳操作，发送对应的选举(`MessageType_MsgHup`)/心跳请求(`MessageType_MsgBeat`)至step，由step具体触发具体的选举和心跳操作。
2. 选举，入口`startElection`，按照论文所述，主要注意`lastLogIndex`，`lastLogTerm`的获得，这里需要先修改`raftlog`。然后对每个peer（不包含自己）发送`sendRequestVote`。这里注意需要重置选举的时间和超时时间，超时时间的设置可以参考`testNonleaderElectionTimeoutRandomized`里面写的范围(timeout, 2*timeout)。然后也是由step函数处理`RequestVote`，对应论文中的选举规则。我们综合了论文和doc.go的思路，最后实现的结果综合了两个。最后返回选举结果给发送者。发送者会统计收到的票数，发送者统计最终的票数，如果超过半数赞成，则变成Leader，如果超过半数拒绝，则变成follower。
3. 心跳，入口`startElection`，这里与论文不同，本实验将心跳与appendEntries分开了，所以该函数来说十分简单，Leader仅需定时发送自己的心跳请求即可，接收者根据心跳请求是否正确，来重置自己的选举时间、timeout和leader，返回response给发送者。

### 实验结果

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

## Project2ac

### 实验目的

1. 实现日志复制
2. 通过测试project2ab

### 实验思路

1. 当接收到`MessageType_MsgPropose`这个local msg，leader首先调用`appendEntry`添加日志，然后调用`bcastAppend`方法，向其余节点发送`MessageType_MsgAppend`消息。

2. 当其余节点收到leader的`MessageType_MsgAppend`消息，状态转为follower，具体处理细节参照raft论文。需要值得注意的是，follower处理此消息时，需要依据leader的commitIndex，本地commitIndex和最新日志索引的状态来更新本地commitIndex。但在判断时，最新日志索引不能调用`follower.getLastLogIndex()`来获取，而是需要通过`MessageType_MsgAppend`中`Index`和`Entries`的长度计算得出。当节点处理结束后，向leader节点发送
`MessageType_MsgAppendResponse`消息。

3. 当leader接收到`MessageType_MsgAppendResponse`消息，按照消息更新`raft.Prs`和`raft.RaftLog.committed`,具体细节参照Raft论文。当leader处理结束，发现`Msg.From`节点的commitIndex与leader不一致，则继续发送`MessageType_MsgAppend`消息，直至达成一致状态。同样的，如果leader在处理`MessageType_MsgAppendResponse`消息时，commitIndex发生更新，那么leader会给其余所有节点发送`MessageType_MsgAppend`。

4. 当leader发送心跳并收到其余节点的`MessageType_MsgHeartbeatResponse`消息，leader会判断此follower的commitIndex是否与leader的commitIndex一致，若不一致，那么leader会给该节点发送`MessageType_MsgAppend`。

### 实验结果

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
