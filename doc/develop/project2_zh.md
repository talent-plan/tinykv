# Project2

实验中整体代码逻辑

tick 触发事件 -> 发送请求至step（不经过Msgs）-> step采取对应方法，调用sendxx方法发送request至Msgs（RPC) -> 接收端通过step处理request，调用handlexx方法，返回response至Msgs（RPC）-> 发送端通过step处理response，调用handlexx方法处理

## Project2aa

### 实验目的

1. 完成Raft的领导选举
2. 通过测试`project2aa`

### 实验思路

实验思路从`raft.doc`中获得。

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