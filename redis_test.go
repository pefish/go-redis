package p_redis

import (
	"testing"
)

func TestRedisClass_ConnectWithConfiguration(t *testing.T) {
	redisClient := RedisClass{}
	redisClient.ConnectWithConfiguration(Configuration{
		Host: `127.0.0.1`,
	})
	redisClient.Close()
}

func TestRedisClass_ConnectWithMap(t *testing.T) {
	redisClient := RedisClass{}
	redisClient.ConnectWithMap(map[string]interface{}{
		`host`: `127.0.0.1`,
	})
	redisClient.Close()
}