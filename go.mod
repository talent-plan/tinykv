module github.com/ngaut/unistore

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/coocood/badger v1.5.1-0.20191031155124-64a2ad772588
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/dgryski/go-farm v0.0.0-20190104051053-3adb47b1fb0f
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.1 // indirect
	github.com/juju/errors v0.0.0-20181118221551-089d3ea4e4d5
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/juju/testing v0.0.0-20180920084828-472a3e8b2073 // indirect
	github.com/ngaut/log v0.0.0-20180314031856-b8e36e7ba5ac
	github.com/pierrec/lz4 v2.0.5+incompatible
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/kvproto v0.0.0-20191121022655-4c654046831d
	github.com/pingcap/parser v0.0.0-20190903084634-0daf3f706c76
	github.com/pingcap/tidb v1.1.0-beta.0.20190904060835-0872b65ff1f9
	github.com/pingcap/tipb v0.0.0-20191008064422-018b2fadf414
	github.com/prometheus/client_golang v0.9.0
	github.com/shirou/gopsutil v2.18.10+incompatible
	github.com/stretchr/testify v1.3.0
	github.com/uber-go/atomic v1.3.2
	github.com/zhangjinpeng1987/raft v0.0.0-20190624145930-deeb32d6553d
	golang.org/x/net v0.0.0-20190108225652-1e06a53dbb7e
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc v1.17.0
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	gopkg.in/stretchr/testify.v1 v1.2.2 // indirect
)

replace go.etcd.io/etcd => github.com/zhangjinpeng1987/etcd v0.0.0-20190226085253-137eac022b64

go 1.13
