module github.com/ngaut/unistore

require (
	github.com/coocood/badger v1.5.1-0.20190823064801-dda2a0ccea7f
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
	github.com/pingcap/kvproto v0.0.0-20190724165112-ec9df5f208a7
	github.com/pingcap/parser v0.0.0-20190822024127-41d48df05864
	github.com/pingcap/tidb v0.0.0-20190823060240-0ff5b50c1509
	github.com/pingcap/tipb v0.0.0-20190806070524-16909e03435e
	github.com/prometheus/client_golang v0.9.0
	github.com/shirou/gopsutil v2.18.10+incompatible
	github.com/stretchr/testify v1.3.0
	github.com/uber-go/atomic v1.3.2
	github.com/zhangjinpeng1987/raft v0.0.0-20190624145930-deeb32d6553d
	go.etcd.io/etcd v0.0.0-20190320044326-77d4b742cdbf
	golang.org/x/net v0.0.0-20190108225652-1e06a53dbb7e
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc v1.17.0
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
)

replace go.etcd.io/etcd => github.com/zhangjinpeng1987/etcd v0.0.0-20190226085253-137eac022b64
