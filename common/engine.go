package common

import (
	"time"

	"github.com/BigbearO/mem_cache/engine/payload"
	"github.com/BigbearO/mem_cache/redis/protocol"
)

type Engine interface {
	Exec(c Connection, redisCommand [][]byte) (result protocol.Reply)
	ForEach(dbIndex int, cb func(key string, data *payload.DataEntity, expiration *time.Time) bool)
	Close()
}
