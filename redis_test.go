package go_redis

import (
	"fmt"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestRedisClass_ConnectWithConfiguration(t *testing.T) {
	RedisInstance.MustConnect(Configuration{
		Address: `127.0.0.1`,
	})
	RedisInstance.Close()
}

func Test_StringClass_SetNx(t *testing.T) {
	RedisInstance.MustConnect(Configuration{
		Address: `127.0.0.1`,
	})
	bool_ := RedisInstance.String.MustSetNx(`test_str`, `haha`, 2*time.Second)
	if !bool_ {
		t.Error()
	}
	bool3_ := RedisInstance.String.MustSetNx(`test_str`, `haha`, 2*time.Second)
	if bool3_ {
		t.Error()
	}
	time.Sleep(3 * time.Second)
	bool1_ := RedisInstance.String.MustSetNx(`test_str`, `haha`, 2*time.Second)
	if !bool1_ {
		t.Error()
	}
	RedisInstance.Close()
}

func Test_SetClass_Sadd(t *testing.T) {
	RedisInstance.MustConnect(Configuration{
		Address: `127.0.0.1`,
	})
	RedisInstance.Set.MustSadd(`test_set`, `haha`)
}

func Test_SetClass_SisMember(t *testing.T) {
	RedisInstance.MustConnect(Configuration{
		Address: `127.0.0.1`,
	})
	result := RedisInstance.Set.MustSisMember(`test_set`, `haha`)
	fmt.Println(result)
}

func TestRedisClass_GetLock(t *testing.T) {
	RedisInstance.MustConnect(Configuration{
		Address: `127.0.0.1`,
	})
	key := `haha`
	rid := uuid.New().String()
	if !RedisInstance.MustGetLock(key, rid, 5*time.Second) {
		fmt.Println(`获取锁失败`)
		return
	}
	defer RedisInstance.MustReleaseLock(key, rid)
	time.Sleep(6 * time.Second)
	fmt.Println(`获取锁成功`)
}

func Test__ListClass_ListAll(t *testing.T) {
	RedisInstance.MustConnect(Configuration{
		Address: `127.0.0.1`,
	})
	RedisInstance.List.MustRPush("test_list1", "1")
	RedisInstance.List.MustRPush("test_list1", "2")
	RedisInstance.List.MustRPush("test_list1", "3")

	result := RedisInstance.List.MustListAll("test_list1")
	fmt.Println(result)

	result1 := RedisInstance.List.MustListAll("test_list10")
	fmt.Println(result1)
}
