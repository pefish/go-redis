package p_redis

import (
	"testing"
	"time"
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

func Test_StringClass_SetNx(t *testing.T) {
	redisClient := RedisClass{}
	redisClient.ConnectWithMap(map[string]interface{}{
		`host`: `127.0.0.1`,
	})
	bool_ := redisClient.String.SetNx(`test`, `haha`, 2 * time.Second)
	if !bool_ {
		t.Error()
	}
	bool3_ := redisClient.String.SetNx(`test`, `haha`, 2 * time.Second)
	if bool3_ {
		t.Error()
	}
	time.Sleep(3 * time.Second)
	bool1_ := redisClient.String.SetNx(`test`, `haha`, 2 * time.Second)
	if !bool1_ {
		t.Error()
	}
	redisClient.Close()
}