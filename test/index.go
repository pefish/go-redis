package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/pefish/go-redis"
	"time"
)

func main() {
	redisClient := go_redis.RedisClass{}
	redisClient.ConnectWithMap(map[string]interface{}{
		`host`: `127.0.0.1`,
	})
	key := `haha`
	rid := uuid.New().String()
	if !redisClient.GetLock(key, rid, 5 * time.Second) {
		fmt.Println(`获取锁失败`)
		return
	}
	defer redisClient.ReleaseLock(key, rid)
	time.Sleep(10 * time.Second)
	fmt.Println(`获取锁成功`)
}
