package go_redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	i_logger "github.com/pefish/go-interface/i-logger"
	go_test_ "github.com/pefish/go-test"
)

var RedisInstance *RedisType

func init() {
	RedisInstance = NewRedisInstance(&i_logger.DefaultLogger)
	RedisInstance.Connect(&Configuration{
		Url:      `127.0.0.1`,
		Password: "password",
	})
}

func TestRedisClass_ConnectWithConfiguration(t *testing.T) {
	RedisInstance.Close()
}

func Test_StringClass_SetNx(t *testing.T) {
	bool_, err := RedisInstance.String.SetNx(`test_str`, `haha`, 2*time.Second)
	go_test_.Equal(t, nil, err)
	if !bool_ {
		t.Error()
	}
	bool3_, err := RedisInstance.String.SetNx(`test_str`, `haha`, 2*time.Second)
	go_test_.Equal(t, nil, err)
	if bool3_ {
		t.Error()
	}
	time.Sleep(3 * time.Second)
	bool1_, err := RedisInstance.String.SetNx(`test_str`, `haha`, 2*time.Second)
	go_test_.Equal(t, nil, err)
	if !bool1_ {
		t.Error()
	}
	RedisInstance.Close()
}

func Test_SetClass_Sadd(t *testing.T) {
	err := RedisInstance.Set.Add(`test_set`, `haha`)
	go_test_.Equal(t, nil, err)
}

func Test_SetClass_SisMember(t *testing.T) {
	result, err := RedisInstance.Set.IsMember(`test_set`, `haha`)
	go_test_.Equal(t, nil, err)
	fmt.Println(result)
}

func TestRedisClass_GetLock(t *testing.T) {
	key := `haha`
	rid := uuid.New().String()
	getLockResult, err := RedisInstance.GetLock(key, rid, 5*time.Second)
	go_test_.Equal(t, nil, err)
	if !getLockResult {
		fmt.Println(`获取锁失败`)
		return
	}
	defer RedisInstance.ReleaseLock(key, rid)
	time.Sleep(6 * time.Second)
	fmt.Println(`获取锁成功`)
}

func Test__ListClass_ListAll(t *testing.T) {
	RedisInstance.List.RPush("test_list1", "1")
	RedisInstance.List.RPush("test_list1", "2")
	RedisInstance.List.RPush("test_list1", "3")

	result, err := RedisInstance.List.ListAll("test_list1")
	go_test_.Equal(t, nil, err)
	fmt.Println(result)

	result1, err := RedisInstance.List.ListAll("test_list10")
	go_test_.Equal(t, nil, err)
	fmt.Println(result1)
}
