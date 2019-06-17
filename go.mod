module github.com/ngaut/unistore

require (
	github.com/coocood/badger v1.5.1-0.20190617061430-79d109a8fc71
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/dgryski/go-farm v0.0.0-20190104051053-3adb47b1fb0f
	github.com/golang/protobuf v1.3.0
	github.com/juju/errors v0.0.0-20181118221551-089d3ea4e4d5
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/juju/testing v0.0.0-20180920084828-472a3e8b2073 // indirect
	github.com/ngaut/log v0.0.0-20180314031856-b8e36e7ba5ac
	github.com/pierrec/lz4 v2.0.5+incompatible
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.1
	github.com/pingcap/kvproto v0.0.0-20190227013052-e71ca0165a5f
	github.com/pingcap/parser v0.0.0-20190325012055-cc0fa08f99ca
	github.com/pingcap/pd v2.1.0-rc.4+incompatible
	github.com/pingcap/tidb v0.0.0-20190325083614-d6490c1cab3a
	github.com/pingcap/tipb v0.0.0-20190107072121-abbec73437b7
	github.com/shirou/gopsutil v2.18.10+incompatible
	github.com/stretchr/testify v1.3.0
	github.com/uber-go/atomic v1.3.2
	github.com/zhangjinpeng1987/raft v0.0.0-20190610095602-72a63e7f0277
	go.etcd.io/etcd v3.3.12+incompatible
	golang.org/x/net v0.0.0-20190108225652-1e06a53dbb7e
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc v1.17.0
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
)

replace go.etcd.io/etcd => github.com/zhangjinpeng1987/etcd v0.0.0-20190226085253-137eac022b64
