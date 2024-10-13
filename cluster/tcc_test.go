package cluster

import (
	"testing"
	"time"

	"github.com/BigbearO/mem_cache/aof"
	"github.com/BigbearO/mem_cache/redis/connection"
	"github.com/BigbearO/mem_cache/redis/protocol"
	"github.com/BigbearO/mem_cache/tool/conf"
)

var cluster map[string]*Cluster = make(map[string]*Cluster)

type mockFactory struct {
}

func (f *mockFactory) GetConn(addr string) (Client, error) {
	return &fakeClient{cluster: cluster[addr]}, nil
}

func (f *mockFactory) ReturnConn(peer string, cli Client) error {
	return nil
}

type fakeClient struct {
	cluster *Cluster
}

func (f *fakeClient) Send(command [][]byte) (protocol.Reply, error) {

	return f.cluster.Exec(connection.NewVirtualConn(), command), nil
}

func TestTCC(t *testing.T) {
	conf.GlobalConfig.Peers = []string{"127.0.0.1:6379", "127.0.0.1:7379", "127.0.0.1:8379"}

	for _, v := range conf.GlobalConfig.Peers {
		conf.GlobalConfig.Self = v
		clusterX := NewCluster()
		clusterX.clientFactory = &mockFactory{}
		cluster[v] = clusterX
	}

	// 选中一个节点，作为协调者
	oneCluster := cluster[conf.GlobalConfig.Peers[0]]
	conn := connection.NewVirtualConn()

	txId := oneCluster.newTxId()
	keys := []string{"1", "6", "10"}
	values := make(map[string]string)
	values["1"] = "300"
	values["6"] = "300"
	values["10"] = "300"

	ipMap := oneCluster.groupByKeys(keys)
	for ip, keys := range ipMap {
		// txid mset key value [key value...]
		argsGroup := [][]byte{[]byte(txId), []byte("mset")}
		for _, key := range keys {
			argsGroup = append(argsGroup, []byte(key), []byte(values[key]))
		}
		//发送命令： prepare txid mset key value [key value...]
		oneCluster.Relay(ip, conn, pushCmd(argsGroup, "Prepare"))
	}

	// test commit
	commitTransaction(oneCluster, conn, txId, ipMap)
	t.Logf("%q", oneCluster.Exec(conn, aof.Get([]byte("1"))).ToBytes())
	t.Logf("%q", oneCluster.Exec(conn, aof.Get([]byte("6"))).ToBytes())
	t.Logf("%q", oneCluster.Exec(conn, aof.Get([]byte("10"))).ToBytes())
	time.Sleep(1 * time.Second)
	// test rollback
	rollbackTransaction(oneCluster, conn, txId, ipMap)
	t.Logf("%q", oneCluster.Exec(conn, aof.Get([]byte("1"))).ToBytes())
	t.Logf("%q", oneCluster.Exec(conn, aof.Get([]byte("6"))).ToBytes())
	t.Logf("%q", oneCluster.Exec(conn, aof.Get([]byte("10"))).ToBytes())
}
