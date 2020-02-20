package test_raftstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/log"
)

func init() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	log.SetLevelByString("info")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 100 * time.Millisecond

// a client runs the function f and then signals it is done
func run_client(t *testing.T, me int, ca chan bool, fn func(me int, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()
	fn(me, t)
	ok = true
}

// spawn ncli clients and wait until they are all done
func spawn_clients_and_wait(t *testing.T, ch chan bool, ncli int, fn func(me int, t *testing.T)) {
	defer func() { ch <- true }()
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go run_client(t, cli, ca[cli], fn)
	}
	// log.Printf("spawn_clients_and_wait: waiting for clients")
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		// log.Infof("spawn_clients_and_wait: client %d is done\n", cli)
		if ok == false {
			t.Fatalf("failure")
		}
	}

}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

// check that for a specific client all known appends are present in a value,
// and in order
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
		}
		off1 := strings.LastIndex(v, wanted)
		if off1 != off {
			t.Fatalf("duplicate element %v in Append result", wanted)
		}
		if off <= lastoff {
			t.Fatalf("wrong order for element %v in Append result", wanted)
		}
		lastoff = off
	}
}

// check that all known appends are present in a value,
// and are in order for each concurrent client.
func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("%v missing element %v in Append result %v", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element %v in Append result", wanted)
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element %v in Append result", wanted)
			}
			lastoff = off
		}
	}
}

// repartition the servers periodically
func partitioner(t *testing.T, cluster *Cluster, ch chan bool, done *int32, unreliable bool) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 {
		a := make([]int, cluster.count)
		for i := 0; i < cluster.count; i++ {
			a[i] = (rand.Int() % 2)
		}
		pa := make([][]uint64, 2)
		for i := 0; i < 2; i++ {
			pa[i] = make([]uint64, 0)
			for j := 1; j <= cluster.count; j++ {
				if a[j-1] == i {
					pa[i] = append(pa[i], uint64(j))
				}
			}
		}
		cluster.ClearFilters()
		log.Infof("partition: %v, %v", pa[0], pa[1])
		cluster.AddFilter(&PartitionFilter{
			s1: pa[0],
			s2: pa[1],
		})
		if unreliable {
			cluster.AddFilter(&DropFilter{})
		}
		time.Sleep(electionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
	}
}

// Basic test is as follows: one or more clients submitting Append/Get
// operations to set of servers for some period of time.  After the period is
// over, test checks that all appended values are present and in order for a
// particular key.  If unreliable is set, RPCs may fail.  If crash is set, the
// servers crash after the period is over and restart.  If partitions is set,
// the test repartitions the network concurrently with the clients and servers. If
// maxraftstate is a positive number, the size of the state for Raft (i.e., log
// size) shouldn't exceed 2*maxraftstate.
func GenericTest(t *testing.T, part string, nclients int, unreliable bool, crash bool, partitions bool, maxraftstate int) {

	title := "Test: "
	if unreliable {
		// the network drops RPC requests and replies.
		title = title + "unreliable net, "
	}
	if crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + " (" + part + ")" // 3A or 3B

	nservers := 5
	cluster := NewTestCluster(nservers)

	cluster.Start()
	defer cluster.Shutdown()

	done_partitioner := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)
	ch_clients := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int, 1)
	}
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go spawn_clients_and_wait(t, ch_clients, nclients, func(cli int, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			for atomic.LoadInt32(&done_clients) == 0 {
				if (rand.Int() % 1000) < 500 {
					key := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", j)
					value := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Infof("%d: client new put %v,%v\n", cli, key, value)
					cluster.MustPut([]byte(key), []byte(value))
					last = NextValue(last, value)
					j++
				} else {
					start := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", 0)
					end := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", j)
					// log.Infof("%d: client new scan %v-%v\n", cli, start, end)
					values := cluster.Scan([]byte(start), []byte(end))
					v := string(bytes.Join(values, []byte("")))
					if v != last {
						log.Fatalf("get wrong value, client %v\nwant:%v\ngot: %v\n", cli, last, v)
					}
				}
			}
		})

		if partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(300 * time.Millisecond)
			go partitioner(t, cluster, ch_partitioner, &done_partitioner, unreliable)
		}
		time.Sleep(1 * time.Second)
		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit
		if partitions {
			// log.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cluster.ClearFilters()
			if unreliable {
				cluster.AddFilter(&DropFilter{})
			}
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		// log.Printf("wait for clients\n")
		<-ch_clients

		if crash {
			log.Warnf("shutdown servers\n")
			for i := 1; i <= nservers; i++ {
				cluster.StopServer(uint64(i))
			}
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			log.Warnf("restart servers\n")
			// crash and re-start all
			for i := 1; i <= nservers; i++ {
				cluster.StartServer(uint64(i))
			}
		}

		for cli := 0; cli < nclients; cli++ {
			// log.Printf("read from clients %d\n", cli)
			j := <-clnts[cli]

			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			start := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", 0)
			end := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", j)
			values := cluster.Scan([]byte(start), []byte(end))
			v := string(bytes.Join(values, []byte("")))
			checkClntAppends(t, cli, v, j)

			for k := 0; k < j; k++ {
				key := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", k)
				cluster.MustDelete([]byte(key))
			}
		}

		// if maxraftstate > 0 {
		// 	// Check maximum after the servers have processed all client
		// 	// requests and had time to checkpoint.
		// 	if cfg.LogSize() > 2*maxraftstate {
		// 		t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
		// 	}
		// }
	}
}

// func TestBasic3A(t *testing.T) {
// 	// Test: one client (3A) ...
// 	GenericTest(t, "3A", 1, false, false, false, -1)
// }

// func TestConcurrent3A(t *testing.T) {
// 	// Test: many clients (3A) ...
// 	GenericTest(t, "3A", 5, false, false, false, -1)
// }

// func TestUnreliable3A(t *testing.T) {
// 	// Test: unreliable net, many clients (3A) ...
// 	GenericTest(t, "3A", 5, true, false, false, -1)
// }

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
func TestOnePartition3A(t *testing.T) {
	cluster := NewTestCluster(5)
	cluster.Start()
	defer cluster.Shutdown()

	regionID := cluster.GetRegion([]byte("")).GetId()
	// transfer leader to (1, 1)
	peer := NewPeer(1, 1)
	cluster.MustTransferLeader(regionID, &peer)

	// leader in majority, partition doesn't affect write/read
	cluster.AddFilter(&PartitionFilter{
		s1: []uint64{1, 2, 3},
		s2: []uint64{4, 5},
	})
	cluster.MustPut([]byte("k1"), []byte("v1"))
	cluster.MustGet([]byte("k1"), []byte("v1"))
	MustGetNone(cluster.engines[4], []byte("k1"))
	MustGetNone(cluster.engines[5], []byte("k1"))
	cluster.MustTransferLeader(regionID, &peer)
	cluster.ClearFilters()

	// old leader in minority, new leader should be elected
	cluster.AddFilter(&PartitionFilter{
		s1: []uint64{1, 2},
		s2: []uint64{3, 4, 5},
	})
	cluster.MustGet([]byte("k1"), []byte("v1"))
	cluster.MustPut([]byte("k1"), []byte("changed"))
	MustGetEqual(cluster.engines[1], []byte("k1"), []byte("v1"))
	MustGetEqual(cluster.engines[2], []byte("k1"), []byte("v1"))
	cluster.ClearFilters()

	// when partition heals, old leader should sync data
	cluster.MustPut([]byte("k2"), []byte("v2"))
	MustGetEqual(cluster.engines[1], []byte("k2"), []byte("v2"))
	MustGetEqual(cluster.engines[1], []byte("k1"), []byte("changed"))
}

// func TestManyPartitionsOneClient3A(t *testing.T) {
// 	// Test: partitions, one client (3A) ...
// 	GenericTest(t, "3A", 1, false, false, true, -1)
// }

// func TestManyPartitionsManyClients3A(t *testing.T) {
// 	// Test: partitions, many clients (3A) ...
// 	GenericTest(t, "3A", 5, false, false, true, -1)
// }

// func TestPersistOneClient3A(t *testing.T) {
// 	// Test: restarts, one client (3A) ...
// 	GenericTest(t, "3A", 1, false, true, false, -1)
// }

// func TestPersistConcurrent3A(t *testing.T) {
// 	// Test: restarts, many clients (3A) ...
// 	GenericTest(t, "3A", 5, false, true, false, -1)
// }

// func TestPersistConcurrentUnreliable3A(t *testing.T) {
// 	// Test: unreliable net, restarts, many clients (3A) ...
// 	GenericTest(t, "3A", 5, true, true, false, -1)
// }

// func TestPersistPartition3A(t *testing.T) {
// 	// Test: restarts, partitions, many clients (3A) ...
// 	GenericTest(t, "3A", 5, false, true, true, -1)
// }

func TestPersistPartitionUnreliable3A(t *testing.T) {
	// Test: unreliable net, restarts, partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, true, true, true, -1)
}
