# Project2


## Project2a

实验中整体代码逻辑

tick 触发事件 -> 发送请求至step（不经过Msgs）-> step采取对应方法，调用sendxx方法发送request至Msgs（RPC) -> 接收端通过step处理request，调用handlexx方法，返回response至Msgs（RPC）-> 发送端通过step处理response，调用handlexx方法处理

### Project2aa

#### 实验目的

1. 完成Raft的领导选举
2. 通过测试`project2aa`

#### 实验思路

实验思路从`raft/doc.go`中获得。

1. 根据实验手册的简单提示，我们可以从`raft.Raft.tick()`，它是一个逻辑时钟，由Node推进，不过暂时2aa里用不到，所以我们只需要一开始实现`tick()`函数即可。不同state的raft，对应时钟有不同的行为：即`Follower`和`Candidate`只有选举的行为，而`Leader`只有心跳的行为，根据doc.go前两个rpc的提示(`MessageType_MsgHup`, `MessageType_MsgBeat`)实现`tickElection`和`tickHeartbeat`，这两个函数触发触发选举/心跳操作，发送对应的选举(`MessageType_MsgHup`)/心跳请求(`MessageType_MsgBeat`)至step，由step具体触发具体的选举和心跳操作。
2. 选举，入口`startElection`，按照论文所述，主要注意`lastLogIndex`，`lastLogTerm`的获得，这里需要先修改`raftlog`。然后对每个peer（不包含自己）发送`sendRequestVote`。这里注意需要重置选举的时间和超时时间，超时时间的设置可以参考`testNonleaderElectionTimeoutRandomized`里面写的范围(timeout, 2*timeout)。然后也是由step函数处理`RequestVote`，对应论文中的选举规则。我们综合了论文和doc.go的思路，最后实现的结果综合了两个。最后返回选举结果给发送者。发送者会统计收到的票数，发送者统计最终的票数，如果超过半数赞成，则变成Leader，如果超过半数拒绝，则变成follower。
3. 心跳，入口`startElection`，这里与论文不同，本实验将心跳与appendEntries分开了，所以该函数来说十分简单，Leader仅需定时发送自己的心跳请求即可，接收者根据心跳请求是否正确，来重置自己的选举时间、timeout和leader，返回response给发送者。

### Project2ab

#### 实验目的

1. 实现日志复制
2. 通过测试project2ab

#### 实验思路

1. 当接收到`MessageType_MsgPropose`这个local msg，leader首先调用`appendEntry`添加日志，然后调用`bcastAppend`方法，向其余节点发送`MessageType_MsgAppend`消息。

2. 当其余节点收到leader的`MessageType_MsgAppend`消息，状态转为follower，具体处理细节参照raft论文。需要值得注意的是，follower处理此消息时，需要依据leader的commitIndex，本地commitIndex和最新日志索引的状态来更新本地commitIndex。但在判断时，最新日志索引不能调用`follower.getLastLogIndex()`来获取，而是需要通过`MessageType_MsgAppend`中`Index`和`Entries`的长度计算得出。当节点处理结束后，向leader节点发送
`MessageType_MsgAppendResponse`消息。

3. 当leader接收到`MessageType_MsgAppendResponse`消息，按照消息更新`raft.Prs`和`raft.RaftLog.committed`,具体细节参照Raft论文。当leader处理结束，发现`Msg.From`节点的commitIndex与leader不一致，则继续发送`MessageType_MsgAppend`消息，直至达成一致状态。同样的，如果leader在处理`MessageType_MsgAppendResponse`消息时，commitIndex发生更新，那么leader会给其余所有节点发送`MessageType_MsgAppend`。

4. 当leader发送心跳并收到其余节点的`MessageType_MsgHeartbeatResponse`消息，leader会判断此follower的commitIndex是否与leader的commitIndex一致，若不一致，那么leader会给该节点发送`MessageType_MsgAppend`。

### Project2ac

#### 实验目的

1. 实现rawnode中的`Ready()`、`HasReady()`、`Advance()`函数
2. 通过测试project2ac

#### 实验思路

`Ready`结构体内对它的所有参数解释已经非常全面了

`Ready()`：返回一个Ready结构体，包含对应的参数

`HasReady()`：根据Raft判断是否应该生成Ready结构体，当然这里的判断标准有很多。首先要看`softState`和`hardState`，这两个结构体详细内容已经在Ready中得到说明，简单说就是这两个结构体如果与上次不同则返回`true`。同时需要判断`CommittedEntries`是否为空，如果不为空则返回`true`，这个属性属于日志中已经提交了但是还没应用到状态机中的日志。同时也需要判断`Entries`是否为空，如果不为空返回`true`，这个属性属于日志中需要持久化的日志，即stabled之后的。其他情况返回`false`

`Advance(rd Ready)`：这个函数接受一个`Ready`，主要用来处理`Ready`，在本实验中只需要处理`CommittedEntries`，并更新raft中的`stable`就可以了，还有`Entries`更新`applied`即可。但是本身需要处理更多内容，这是后面实验的内容了。


## Project2b

### 实验目的

1. 实现`PeerStorage.SaveReadyState`和`PeerStorage.Append`方法
2. 实现`proposeRaftCommand`和`HandleRaftReady`方法
3. 通过测试project2b

### 实验细节

在tinkv的设计中，一个`Store`可以拥有多个Raft节点，即`Peer`，而这些`Peer`又隶属于不同的`Region`。对于单个`Peer`而言，若节点的状态发生改变，即`HasReady()`返回true，节点需要将State、日志和快照持久化，然后节点处理并回复上层传递的Msg。

持久化State、日志和快照时，会利用底层的`badger`创建的两个数据库，raftDB和kvDB，具体存储细节如表所示：

| Key            | KeyFormat                      | Value          | DB |
|:----           |:----                           |:----           |:---|
|raft_log_key    |0x01 0x02 region_id 0x01 log_idx|Entry           |raft|
|raft_state_key  |0x01 0x02 region_id 0x02        |RaftLocalState  |raft|
|apply_state_key |0x01 0x02 region_id 0x03        |RaftApplyState  |kv  |
|region_state_key|0x01 0x03 region_id 0x01        |RegionLocalState|kv  |

本实验，日志的持久化在`PeerStorage.Append`方法中完成，在存储时，需要注意一下几个要点：
1. 仅存储未存储的最新的log
2. 若raftDB存储的未被commit的log，存在将来永远不会被commit的情况，需要从raftDB中删除
3. log存储完成后，需要更新`PeerStorage.raftState`，因为此时`raftState`的log索引和term均已发生改变

`PeerStorage.SaveReadyState`是对持久化State、日志和快照的一个封装。日志的持久化调用`PeerStorage.Append`方法。持久化State前，需要依据`raft.Ready`更新`PeerStorage.raftState`，然后将`PeerStorage.raftState`写入raftDB中。快照的持久化在后续实验完成。

来自客户端的请求，主要由`peerMsgHandler`处理，主要有两个功能：一个是`HandleMsgs`，另一个是`HandleRaftReady`。

`HandleMsgs`处理从 raftCh 接收到的所有消息，本实验我们只需要关注`message.MsgTypeRaftCmd`，将此类消息由`proposeRaftCommand`方法处理后，再发送到raft集群内。

实现`proposeRaftCommand`时，需要注意以下要点：
1. 判断消息合法性，如果key不属于当前raftRegion，`message.Callback`返回`ErrResp`
2. 对消息所包含的数据序列化
3. 将此消息的ProposalIndex和Term，以及`message.Callback`以`proposal`结构体的形式记录下来
4. 把序列化后的数据，由`propose`函数封装为日志

消息处理完后，Raft节点会有一些状态更新，`HandleRaftReady`
从Raft模块准备好并执行相应的操作，主要包括：
1. 若raft节点状态发生改变，调用`PeerStorage.SaveReadyState`进行持久化
2. 将消息转发给raft，然后对raft中已经commit的log，处理并按照log类型，在数据库中执行相应操作
3. 每一条log都对应一条`proposal`，处理log时，需要删除这条log之前的所有propoal，因为这些propoal对应的log永远不会commit
4. 对log在数据库中执行结束后，按照操作类型，callback相应的resp
