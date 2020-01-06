# PD

[![TravisCI Build Status](https://travis-ci.org/pingcap/pd.svg?branch=master)](https://travis-ci.org/pingcap/pd)
![GitHub release](https://img.shields.io/github/release/pingcap/pd.svg)
[![CircleCI Build Status](https://circleci.com/gh/pingcap/pd.svg?style=shield)](https://circleci.com/gh/pingcap/pd)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap-incubator/tinykv/pd)](https://goreportcard.com/report/github.com/pingcap-incubator/tinykv/pd)
[![codecov](https://codecov.io/gh/pingcap/pd/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/pd)

PD is the abbreviation for Placement Driver. It is used to manage and schedule the [TiKV](https://github.com/tikv/tikv) cluster.

PD supports distribution and fault-tolerance by embedding [etcd](https://github.com/etcd-io/etcd).

## Build

1. Make sure [​*Go*​](https://golang.org/) (version 1.12) is installed.
2. Use `make` to install PD. PD is installed in the `bin` directory.

## Usage

### Command flags

See [PD Configuration Flags](https://pingcap.com/docs/dev/reference/configuration/pd-server/configuration/#pd-configuration-flags).

### Single Node with default ports

You can run `pd-server` directly on your local machine, if you want to connect to PD from outside,
you can let PD listen on the host IP.

```bash
# Set correct HostIP here.
export HostIP="192.168.199.105"

pd-server --name="pd" \
          --data-dir="pd" \
          --client-urls="http://${HostIP}:2379" \
          --peer-urls="http://${HostIP}:2380" \
          --log-file=pd.log
```

Using `curl` to see PD member:

```bash
curl http://${HostIP}:2379/v2/members

{"members":[{"id":"f62e88a6e81c149","name":"pd","peerURLs":["http://192.168.199.105:2380"],"clientURLs":["http://192.168.199.105:2379"]}]}
```

A better tool [httpie](https://github.com/jkbrzt/httpie) is recommended:

```bash
http http://${HostIP}:2379/v2/members
HTTP/1.1 200 OK
Content-Length: 144
Content-Type: application/json
Date: Thu, 21 Jul 2016 09:37:12 GMT
X-Etcd-Cluster-Id: 33dc747581249309

{
    "members": [
        {
            "clientURLs": [
                "http://192.168.199.105:2379"
            ],
            "id": "f62e88a6e81c149",
            "name": "pd",
            "peerURLs": [
                "http://192.168.199.105:2380"
            ]
        }
    ]
}
```

### Docker

You can use the following command to build a PD image directly:

```bash
docker build -t pingcap/pd .
```

Or you can also use following command to get PD from Docker hub:

```bash
docker pull pingcap/pd
```

Run a single node with Docker:

```bash
# Set correct HostIP here.
export HostIP="192.168.199.105"

docker run -d -p 2379:2379 -p 2380:2380 --name pd pingcap/pd \
          --name="pd" \
          --data-dir="pd" \
          --client-urls="http://0.0.0.0:2379" \
          --advertise-client-urls="http://${HostIP}:2379" \
          --peer-urls="http://0.0.0.0:2380" \
          --advertise-peer-urls="http://${HostIP}:2380" \
          --log-file=pd.log
```

### Cluster

PD is a component in TiDB project, you must run it with TiDB and TiKV together, see
[TiDB-Ansible](https://pingcap.com/docs/dev/how-to/deploy/orchestrated/ansible/#deploy-tidb-using-ansible)
to learn how to set up the cluster and run them.

You can also use [Docker](https://pingcap.com/docs/dev/how-to/deploy/orchestrated/docker/#deploy-tidb-using-docker)
to run the cluster.
