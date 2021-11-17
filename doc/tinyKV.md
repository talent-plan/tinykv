## TinyKV学习日记&RUST&分布式存储



### 学习参考整理

- [github课程地址](https://github.com/tidb-incubator/tinykv)
- [PingCAP 社区问题沟通](https://asktug.com/)
- [reading list](https://github.com/tidb-incubator/tinykv/blob/course/doc/reading_list.md)
- [PCTP课程](https://learn.pingcap.com/learner/course/390002)
- [TiKV源码解析文章](https://pingcap.com/zh/blog/?tag=TiKV%20%E6%BA%90%E7%A0%81%E8%A7%A3%E6%9E%90)
- [6.824](https://pdos.csail.mit.edu/6.824/)
- [6.824视频](https://www.bilibili.com/video/BV1R7411t71W?from=search&seid=16793894489492355812&spm_id_from=333.337.0.0)
- [6.824lab](https://github.com/LebronAl/MIT6.824-2021)
- [rCore](https://github.com/rcore-os/rCore)
- [rust电子书官方中文版](https://kaisery.github.io/trpl-zh-cn/title-page.html)
- [rust社区](https://users.rust-lang.org/)
- [rust编程之道](https://github.com/ZhangHanDong/tao-of-rust-codes)
- [华科参考](https://hustport.com/t/talent-plan)
- [etcd-raft库解析](https://www.codedump.info/post/20180922-etcd-raft/)
- [raft博士论文翻译](https://github.com/LebronAl/raft-thesis-zh_cn)
- [go_to_cpp](https://github.com/awfeequdng/px_golang2cpp)
- [SQL TO SQL](https://mp.weixin.qq.com/s/QtfEVMOEstrxyu8A9Ymd5w)

---



### 作业评分标准

- 提交方式：talent-plan@tidb.io

- 代码正确性（64%）、性能（20%）、解题思路（16%）

- 一共四个Pro，具体如下：

  - Pro1:StandAloneStorage
  - Pro2:RaftKV
    - 2a:Basic Raft
      - [参考链接]( https://github.com/LX-676655103/Tinykv-2021/blob/course/doc/project2.md)
      - [raft动画演示](https://raft.github.io)
      - [etcd参考](https://github.com/etcd-io/etcd)
      - [TiKV源码阅读2](https://pingcap.com/zh/blog/tikv-source-code-reading-2)
    - 2b:Drive the Raft Module
      - [TiKV源码阅读6](https://pingcap.com/zh/blog/tikv-source-code-reading-6)
      - [TiKV源码阅读17](https://pingcap.com/zh/blog/tikv-source-code-reading-17)
      - [TiKV源码阅读18](https://pingcap.com/zh/blog/tikv-source-code-reading-18)
    - 2c:Log
  - Pro3:MultiRaftKV
    - 3a:Membership Change and Leadership Change
    - 3b:Region Split
      - [TiKV源码阅读20](https://pingcap.com/zh/blog/tikv-source-code-reading-20)
      - [TiKV源码阅读17](https://pingcap.com/zh/blog/tikv-source-code-reading-17)
      - [etcd_server](https://github.com/etcd-io/etcd)
    - 3c:Balance Schedule
  - Pro4:Transaction
    - 4a:2PC
      - [Percolator 和 TiDB 事务算法](https://pingcap.com/zh/blog/percolator-and-txn)
    - 4b:Transaction API
      - [percolator](https://karellincoln.github.io/2018/04/05/percolator-translate/)
      - [TiKV源码阅读11](https://pingcap.com/zh/blog/tikv-source-code-reading-11)
      - [TiKV源码阅读12](https://pingcap.com/zh/blog/tikv-source-code-reading-12)
      - [TiKV源码阅读13](https://pingcap.com/zh/blog/tikv-source-code-reading-13)
    - 4c:MVCC
      - [实现参考](https://github.com/LX-676655103/Tinykv-2021/blob/course/doc/project4.md)

  

