package go_redis

import (
	"fmt"
	"testing"
	"time"
	"github.com/google/uuid"
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
	bool_ := redisClient.String.MustSetNx(`test`, `haha`, 2 * time.Second)
	if !bool_ {
		t.Error()
	}
	bool3_ := redisClient.String.MustSetNx(`test`, `haha`, 2 * time.Second)
	if bool3_ {
		t.Error()
	}
	time.Sleep(3 * time.Second)
	bool1_ := redisClient.String.MustSetNx(`test`, `haha`, 2 * time.Second)
	if !bool1_ {
		t.Error()
	}
	redisClient.Close()
}

func Test_SetClass_Sadd(t *testing.T) {
	redisClient := RedisClass{}
	redisClient.ConnectWithMap(map[string]interface{}{
		`host`: `127.0.0.1`,
	})
	redisClient.Set.MustSadd(`test`, `haha`)
}

func Test_SetClass_SisMember(t *testing.T) {
	redisClient := RedisClass{}
	redisClient.ConnectWithMap(map[string]interface{}{
		`host`: `127.0.0.1`,
	})
	result := redisClient.Set.MustSisMember(`test`, `haha`)
	fmt.Println(result)
}

func TestRedisClass_GetLock(t *testing.T) {
	redisClient := RedisClass{}
	redisClient.ConnectWithMap(map[string]interface{}{
		`host`: `127.0.0.1`,
	})
	key := `haha`
	rid := uuid.New().String()
	if !redisClient.MustGetLock(key, rid, 5 * time.Second) {
		fmt.Println(`获取锁失败`)
		return
	}
	defer redisClient.MustReleaseLock(key, rid)
	time.Sleep(6 * time.Second)
	fmt.Println(`获取锁成功`)
}

