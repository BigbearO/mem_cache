package cluster

import (
	"github.com/BigbearO/mem_cache/common"
	"github.com/BigbearO/mem_cache/redis/protocol"
	"github.com/BigbearO/mem_cache/tool/logger"
)

func (cluster *Cluster) Relay(peer string, conn common.Connection, redisCommand [][]byte) protocol.Reply {

	// ******本地执行******
	if cluster.self == peer {
		//return cluster.engine.Exec(conn, redisCommand)
		return cluster.Exec(conn, redisCommand)
	}

	// ******发送到远端执行******

	client, err := cluster.clientFactory.GetConn(peer) // 从连接池中获取一个连接
	if err != nil {
		logger.Error(err)
		return protocol.NewGenericErrReply(err.Error())
	}

	defer func() {
		cluster.clientFactory.ReturnConn(peer, client) // 归还连接
	}()

	logger.Debugf("命令:%q,转发至ip:%s", protocol.NewMultiBulkReply(redisCommand).ToBytes(), peer)
	reply, err := client.Send(redisCommand) // 发送命令
	if err != nil {
		logger.Error(err)
		return protocol.NewGenericErrReply(err.Error())
	}

	return reply
}
