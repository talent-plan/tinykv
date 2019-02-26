module github.com/ngaut/unistore

require (
	github.com/coocood/badger v1.5.1-0.20181229021924-c02c9aba9c41
	github.com/cznic/mathutil v0.0.0-20181021201202-eba54fb065b7
	github.com/dgryski/go-farm v0.0.0-20180109070241-2de33835d102
	github.com/golang/protobuf v1.2.0
	github.com/juju/errors v0.0.0-20181118221551-089d3ea4e4d5
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/juju/testing v0.0.0-20180920084828-472a3e8b2073 // indirect
	github.com/ngaut/log v0.0.0-20180314031856-b8e36e7ba5ac
	github.com/pierrec/lz4 v2.0.5+incompatible
	github.com/pingcap/check v0.0.0-20181213055612-5c2b07721bdb
	github.com/pingcap/errors v0.11.0
	github.com/pingcap/kvproto v0.0.0-20190215154024-7f2fc73ef562
	github.com/pingcap/parser v0.0.0-20181218071912-deacf026787e
	github.com/pingcap/tidb v0.0.0-20181130082510-08f0168a6cae
	github.com/pingcap/tipb v0.0.0-20181012112600-11e33c750323
	github.com/stretchr/testify v1.2.2
	go.etcd.io/etcd v3.3.12+incompatible
	golang.org/x/net v0.0.0-20181029044818-c44066c5c816
	golang.org/x/sys v0.0.0-20181025063200-d989b31c8746 // indirect
	golang.org/x/time v0.0.0-20181108054448-85acf8d2951c
	google.golang.org/genproto v0.0.0-20181016170114-94acd270e44e // indirect
	google.golang.org/grpc v1.16.0
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
)

replace github.com/pingcap/tidb v0.0.0-20181130082510-08f0168a6cae => github.com/bobotu/tidb v0.0.0-20181221085922-487bfe4bf561

replace go.etcd.io/etcd => github.com/zhangjinpeng1987/etcd v0.0.0-20190226085253-137eac022b64
