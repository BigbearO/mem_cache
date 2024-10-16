package engine

import (
	"strconv"

	"github.com/BigbearO/mem_cache/common"
	"github.com/BigbearO/mem_cache/redis/protocol"
	"github.com/BigbearO/mem_cache/tool/conf"
)

/*
基础命令
*/
func execSelect(c common.Connection, redisArgs [][]byte) protocol.Reply {
	if len(redisArgs) != 1 {
		return protocol.NewArgNumErrReply("select")
	}
	dbIndex, err := strconv.ParseInt(string(redisArgs[0]), 10, 64)
	if err != nil {
		return protocol.NewGenericErrReply("invaild db index")
	}
	if dbIndex < 0 || dbIndex >= int64(conf.GlobalConfig.Databases) {
		return protocol.NewGenericErrReply("db index out of range")
	}
	c.SetDBIndex(int(dbIndex))
	return protocol.NewOkReply()

}

// 异步方式重写aof
func BGRewriteAOF(engine *Engine) protocol.Reply {
	go engine.aof.Rewrite(newAuxiliaryEngine())
	return protocol.NewSimpleReply("Background append only file rewriting started")
}
