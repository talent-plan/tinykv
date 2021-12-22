module github.com/pingcap-incubator/tinykv

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/Connor1996/badger v1.5.1-0.20210202034640-5ff470f827f8
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/docker/go-units v0.4.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.3
	github.com/google/btree v1.0.0
	github.com/juju/errors v0.0.0-20181118221551-089d3ea4e4d5
	github.com/juju/testing v0.0.0-20200510222523-6c8c298c77a0 // indirect
	github.com/onsi/ginkgo v1.12.1 // indirect
	github.com/onsi/gomega v1.10.0 // indirect
	github.com/opentracing/opentracing-go v1.0.2
	github.com/petar/GoLLRB v0.0.0-20190514000832-33fb24c13b99
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errcode v0.0.0-20180921232412-a1a7271709d9
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/tidb v1.1.0-beta.0.20200309111804-d8264d47f760
	github.com/pingcap/tipb v0.0.0-20200212061130-c4d518eb1d60
	github.com/pkg/errors v0.8.1
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/shirou/gopsutil v2.19.10+incompatible
	github.com/sirupsen/logrus v1.2.0
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.14.0
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	google.golang.org/grpc v1.43.0
	google.golang.org/grpc/examples v0.0.0-20211214234454-7c8a9321b9de // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

go 1.13

replace github.com/pingcap/tidb => github.com/pingcap-incubator/tinysql v0.0.0-20200518090433-a7d00f9e6aa7

replace google.golang.org/grpc => google.golang.org/grpc v1.26.0
