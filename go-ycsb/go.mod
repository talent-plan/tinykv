module github.com/pingcap/go-ycsb

require (
	cloud.google.com/go/spanner v1.1.0
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/XiaoMi/pegasus-go-client v0.0.0-20181029071519-9400942c5d1c
	github.com/aerospike/aerospike-client-go v1.35.2
	github.com/apple/foundationdb/bindings/go v0.0.0-20200112054404-407dc0907f4f
	github.com/boltdb/bolt v1.3.1
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/dgraph-io/badger v1.6.0
	github.com/elastic/go-elasticsearch/v8 v8.3.0
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/go-ini/ini v1.49.0 // indirect
	github.com/go-redis/redis/v9 v9.0.0-beta.1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gocql/gocql v0.0.0-20181124151448-70385f88b28b
	github.com/lib/pq v1.1.1
	github.com/magiconair/properties v1.8.0
	github.com/mattn/go-sqlite3 v2.0.1+incompatible
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/olekukonko/tablewriter v0.0.5
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pkg/errors v0.9.1 // indirect
	github.com/spf13/cobra v1.0.0
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	go.mongodb.org/mongo-driver v1.5.1
	google.golang.org/api v0.87.0
	google.golang.org/genproto v0.0.0-20220718134204-073382fd740c
)

require (
	github.com/aws/aws-sdk-go-v2 v1.16.16
	github.com/aws/aws-sdk-go-v2/config v1.17.8
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.10.0
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.4.26
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.17.1
	github.com/pingcap-incubator/tinykv v0.0.0-20200514052412-e01d729bd45c
	go.etcd.io/etcd/client/pkg/v3 v3.5.6
	go.etcd.io/etcd/client/v3 v3.5.6
)

// require github.com/pingcap/pd v1.1.0-beta.0.20200106144140-f5a7aa985497 // indirect

require (
	cloud.google.com/go v0.103.0 // indirect
	cloud.google.com/go/compute v1.7.0 // indirect
	github.com/AndreasBriese/bbloom v0.0.0-20190306092124-e2d15f34fcf9 // indirect
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/Connor1996/badger v1.5.1-0.20220222053432-2d2cbf472c77 // indirect
	github.com/DataDog/zstd v1.4.1 // indirect
	github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d // indirect
	github.com/aws/aws-sdk-go v1.34.28 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.12.21 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.13.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.13.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.19 // indirect
	github.com/aws/smithy-go v1.13.3 // indirect
	github.com/benbjohnson/clock v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/coocood/bbloom v0.0.0-20190830030839-58deb6228d64 // indirect
	github.com/coocood/rtutil v0.0.0-20190304133409-c84515f646f2 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/dgraph-io/ristretto v0.0.1 // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/elastic/elastic-transport-go/v8 v8.0.0-20211216131617-bbee439d559c // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.1.0 // indirect
	github.com/googleapis/gax-go/v2 v2.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jmhodges/levigo v1.0.0 // indirect
	github.com/juju/errors v0.0.0-20181118221551-089d3ea4e4d5 // indirect
	github.com/klauspost/compress v1.9.5 // indirect
	github.com/klauspost/cpuid v1.2.1 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.3 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/ncw/directio v1.0.4 // indirect
	github.com/ngaut/log v0.0.0-20180314031856-b8e36e7ba5ac // indirect
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/petar/GoLLRB v0.0.0-20190514000832-33fb24c13b99 // indirect
	github.com/pingcap/check v0.0.0-20211026125417-57bd13f7b5f0 // indirect
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00 // indirect
	github.com/pingcap/log v0.0.0-20211215031037-e024ba4eb0ee // indirect
	github.com/pingcap/tidb v1.1.0-beta.0.20200309111804-d8264d47f760 // indirect
	github.com/pingcap/tipb v0.0.0-20200212061130-c4d518eb1d60 // indirect
	github.com/prometheus/client_golang v1.11.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/shirou/gopsutil v3.21.11+incompatible // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/smartystreets/goconvey v0.0.0-20190330032615-68dc04aab96a // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.0.2 // indirect
	github.com/xdg-go/stringprep v1.0.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	github.com/yuin/gopher-lua v0.0.0-20181031023651-12c4817b42c5 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738 // indirect
	go.etcd.io/etcd/api/v3 v3.5.6 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/goleak v1.1.12 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.20.0 // indirect
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a // indirect
	golang.org/x/exp v0.0.0-20220428152302-39d4317da171 // indirect
	golang.org/x/net v0.0.0-20220708220712-1185a9018129 // indirect
	golang.org/x/oauth2 v0.0.0-20220630143837-2104d58473e0 // indirect
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f // indirect
	golang.org/x/sys v0.2.0 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	golang.org/x/tools v0.1.8-0.20211029000441-d6a9af8af023 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/grpc v1.48.0 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.42.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637 // indirect
)

replace github.com/apache/thrift => github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d

replace google.golang.org/grpc => google.golang.org/grpc v1.25.1

replace github.com/pingcap-incubator/tinykv => ../

replace github.com/jmhodges/levigo => ../levigo

replace github.com/tecbot/gorocksdb => ../gorocksdb

replace github.com/pingcap/tidb => github.com/pingcap-incubator/tinysql v0.0.0-20200518090433-a7d00f9e6aa7

go 1.18
