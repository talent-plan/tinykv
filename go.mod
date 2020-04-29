module github.com/pingcap-incubator/tinykv

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/Connor1996/badger v1.5.1-0.20200306031920-9bbcbd8ba570
	github.com/coocood/badger v1.5.1-0.20200224033957-264a46414d6d
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.13+incompatible // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/docker/go-units v0.4.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/lint v0.0.0-20180702182130-06c8688daad7 // indirect
	github.com/golang/protobuf v1.3.4
	github.com/google/btree v1.0.0
	github.com/juju/errors v0.0.0-20181118221551-089d3ea4e4d5
	github.com/montanaflynn/stats v0.0.0-20180911141734-db72e6cae808
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/ngaut/log v0.0.0-20180314031856-b8e36e7ba5ac
	github.com/ngaut/unistore v0.0.0-20200313010927-453cb21d3920
	github.com/opentracing/opentracing-go v1.0.2
	github.com/petar/GoLLRB v0.0.0-20190514000832-33fb24c13b99
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errcode v0.0.0-20180921232412-a1a7271709d9
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/kvproto v0.0.0-20200228095611-2cf9a243b8d5
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/parser v0.0.0-20200305120128-bde9faa0df84
	github.com/pingcap/tidb v1.1.0-beta.0.20200309111804-d8264d47f760
	github.com/pingcap/tipb v0.0.0-20200212061130-c4d518eb1d60
	github.com/pkg/errors v0.8.1
	github.com/shirou/gopsutil v2.19.10+incompatible
	github.com/sirupsen/logrus v1.2.0
	github.com/stretchr/testify v1.4.0
	github.com/struCoder/pidusage v0.1.2 // indirect
	github.com/twinj/uuid v1.0.0 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.14.0
	golang.org/x/net v0.0.0-20200226121028-0de0cce0169b
	google.golang.org/grpc v1.25.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

go 1.13

replace github.com/pingcap/tidb => github.com/pingcap-incubator/tinysql v0.0.0-20200429065743-414e03ef38bd
