package p_redis

import (
	"fmt"
	"gitee.com/pefish/p-go-application"
	"gitee.com/pefish/p-go-error"
	"gitee.com/pefish/p-go-logger"
	"github.com/go-redis/redis"
	"time"
)

type RedisClass struct {
	Db     *redis.Client
	Set    *_SetClass
	List   *_ListClass
	String *_StringClass
	Order  *_OrderClass
	Hash   *_HashClass
}

func (this *RedisClass) Close() {
	if this.Db != nil {
		this.Db.Close()
	}
}

func (this *RedisClass) Connect(host string, port int64, password string, database int64) {
	this.Db = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf(`%s:%d`, host, port),
		Password: password,
		DB:       int(database),
	})
	_, err := this.Db.Ping().Result()
	if err != nil {
		p_error.ThrowError(`redis connect error`, 0, err)
	}

	this.Set = &_SetClass{this.Db}
	this.List = &_ListClass{this.Db}
	this.String = &_StringClass{this.Db}
	this.Order = &_OrderClass{this.Db}
	this.Hash = &_HashClass{this.Db}
}

func (this *RedisClass) Del(key string) {
	if p_application.Application.Debug {
		p_logger.Logger.Debug(fmt.Sprintf(`redis del. key: %s`, key))
	}
	_, err := this.Db.Del(key).Result()
	if err != nil {
		p_error.ThrowError(`redis del error`, 0, err)
	}
}

type _SetClass struct {
	Db *redis.Client
}

func (this *_SetClass) Sadd(key string, value string) {
	if p_application.Application.Debug {
		p_logger.Logger.Debug(fmt.Sprintf(`redis sadd. key: %s, val: %s`, key, value))
	}
	if err := this.Db.SAdd(key, value).Err(); err != nil {
		p_error.ThrowError(`redis sadd error`, 0, err)
	}
}

type _ListClass struct {
	Db *redis.Client
}

type _StringClass struct {
	Db *redis.Client
}

func (this *_StringClass) Set(key string, value string, expiration time.Duration) {
	if p_application.Application.Debug {
		p_logger.Logger.Debug(fmt.Sprintf(`redis set. key: %s, val: %s, expiration: %v`, key, value, expiration))
	}
	if err := this.Db.Set(key, value, expiration).Err(); err != nil {
		p_error.ThrowError(`redis set error`, 0, err)
	}
}

func (this *_StringClass) Get(key string) *string {
	result, err := this.Db.Get(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil
		}
		p_error.ThrowError(`redis get error`, 0, err)
	}
	if p_application.Application.Debug {
		p_logger.Logger.Debug(fmt.Sprintf(`redis get. key: %s, value: %s`, key, result))
	}
	return &result
}

type _OrderClass struct {
	Db *redis.Client
}

type _HashClass struct {
	Db *redis.Client
}

func (this *_HashClass) Hmget(key string, field string) *string {
	if p_application.Application.Debug {
		p_logger.Logger.Debug(fmt.Sprintf(`redis hmget. key: %s, val: %s`, key, field))
	}
	val, err := this.Db.HMGet(key, field).Result()
	if err != nil {
		p_error.ThrowError(`redis hmget error`, 0, err)
	}
	if len(val) == 0 || val[0] == nil {
		return nil
	}
	result := val[0].(string)
	return &result
}
