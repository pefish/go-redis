package p_redis

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/pefish/go-error"
	"github.com/pefish/go-logger"
	"github.com/pefish/go-reflect"
	"time"
)

// ----------------------------- RedisClass -----------------------------

type RedisClass struct {
	Db     *redis.Client
	Set    *_SetClass
	List   *_ListClass
	String *_StringClass
	Order  *_OrderSetClass
	Hash   *_HashClass
}

type Configuration struct {
	Host     string
	Port     interface{}
	Db       interface{}
	Password interface{}
}

func (this *RedisClass) Close() {
	if this.Db != nil {
		err := this.Db.Close()
		if err != nil {
			p_logger.Logger.Error(err)
		} else {
			p_logger.Logger.Info(`redis close succeed.`)
		}
	}
}

func (this *RedisClass) ConnectWithMap(map_ map[string]interface{}) {
	var port uint64 = 6379
	if map_[`port`] != nil {
		port = p_reflect.Reflect.ToUint64(map_[`port`])
	}
	password := ``
	if map_[`password`] != nil {
		password = p_reflect.Reflect.ToString(map_[`password`])
	}
	var database uint64 = 0
	if map_[`db`] != nil {
		database = p_reflect.Reflect.ToUint64(map_[`db`])
	}
	this.Connect(map_[`host`].(string), port, password, database)
}

func (this *RedisClass) ConnectWithConfiguration(configuration Configuration) {
	var port uint64 = 6379
	if configuration.Port != nil {
		port = p_reflect.Reflect.ToUint64(configuration.Port)
	}
	password := ``
	if configuration.Password != nil {
		password = p_reflect.Reflect.ToString(configuration.Password)
	}
	var database uint64 = 0
	if configuration.Db != nil {
		database = p_reflect.Reflect.ToUint64(configuration.Db)
	}
	this.Connect(configuration.Host, port, password, database)
}

func (this *RedisClass) Connect(host string, port uint64, password string, database uint64) {
	address := fmt.Sprintf(`%s:%d`, host, port)
	this.Db = redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       int(database),
	})
	_, err := this.Db.Ping().Result()
	if err != nil {
		p_error.ThrowInternalError(`redis connect error`, err)
	}
	p_logger.Logger.Info(fmt.Sprintf(`redis connect succeed. url: %s`, address))

	this.Set = &_SetClass{this.Db}
	this.List = &_ListClass{this.Db}
	this.String = &_StringClass{this.Db}
	this.Order = &_OrderSetClass{this.Db}
	this.Hash = &_HashClass{this.Db}
}

func (this *RedisClass) Del(key string) {
	p_logger.Logger.Debug(fmt.Sprintf(`redis del. key: %s`, key))
	if err := this.Db.Del(key).Err(); err != nil {
		p_error.ThrowInternalError(`redis del error`, err)
	}
}

func (this *RedisClass) Expire(key string, expiration time.Duration) {
	p_logger.Logger.Debug(fmt.Sprintf(`redis del. key: %s`, key))
	if err := this.Db.Expire(key, expiration).Err(); err != nil {
		p_error.ThrowInternalError(`redis expire error`, err)
	}
}


// ----------------------------- _SetClass -----------------------------

type _SetClass struct {
	Db *redis.Client
}

func (this *_SetClass) Sadd(key string, value string) {
	p_logger.Logger.Debug(fmt.Sprintf(`redis sadd. key: %s, value: %s`, key, value))
	if err := this.Db.SAdd(key, value).Err(); err != nil {
		p_error.ThrowInternalError(`redis sadd error`, err)
	}
}

func (this *_SetClass) Smembers(key string) []string {
	p_logger.Logger.Debug(fmt.Sprintf(`redis smembers. key: %s`, key))
	result, err := this.Db.SMembers(key).Result()
	if err != nil {
		p_error.ThrowInternalError(`redis smembers error`, err)
	}
	return result
}

func (this *_SetClass) SisMembers(key string, member string) bool {
	p_logger.Logger.Debug(fmt.Sprintf(`redis ismember. key: %s, member: %s`, key, member))
	result, err := this.Db.SIsMember(key, member).Result()
	if err != nil {
		p_error.ThrowInternalError(`redis ismember error`, err)
	}
	return result
}

func (this *_SetClass) Srem(key string, members ...interface{}) {
	p_logger.Logger.Debug(fmt.Sprintf(`redis ismember. key: %s, members: %s`, key, members))
	_, err := this.Db.SRem(key, members).Result()
	if err != nil {
		p_error.ThrowInternalError(`redis ismember error`, err)
	}
}


// ----------------------------- _ListClass -----------------------------

type _ListClass struct {
	Db *redis.Client
}


// ----------------------------- _StringClass -----------------------------

type _StringClass struct {
	Db *redis.Client
}

func (this *_StringClass) Set(key string, value string, expiration time.Duration) {
	p_logger.Logger.Debug(fmt.Sprintf(`redis set. key: %s, val: %s, expiration: %v`, key, value, expiration))
	if err := this.Db.Set(key, value, expiration).Err(); err != nil {
		p_error.ThrowInternalError(`redis set error`, err)
	}
}

/**
设置成功返回true
 */
func (this *_StringClass) SetNx(key string, value string, expiration time.Duration) bool {
	p_logger.Logger.Debug(fmt.Sprintf(`redis setnx. key: %s, val: %s, expiration: %v`, key, value, expiration))
	result := this.Db.SetNX(key, value, expiration)
	if err := result.Err(); err != nil {
		p_error.ThrowInternalError(`redis set error`, err)
	}
	return result.Val()
}

func (this *_StringClass) Get(key string) *string {
	result, err := this.Db.Get(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil
		}
		p_error.ThrowInternalError(`redis get error`, err)
	}
	p_logger.Logger.Debug(fmt.Sprintf(`redis get. key: %s, value: %s`, key, result))
	return &result
}


// ----------------------------- _OrderSetClass -----------------------------

type _OrderSetClass struct {
	Db *redis.Client
}


// ----------------------------- _HashClass -----------------------------

type _HashClass struct {
	Db *redis.Client
}

func (this *_HashClass) Hmget(key string, field string) *string {
	val, err := this.Db.HMGet(key, field).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil
		}
		p_error.ThrowInternalError(`redis hmget error`, err)
	}
	if len(val) == 0 || val[0] == nil {
		return nil
	}
	result := val[0].(string)
	p_logger.Logger.Debug(fmt.Sprintf(`redis hmget. key: %s, field: %s, val: %s`, key, field, result))
	return &result
}

func (this *_HashClass) HGet(key, field string) *string {
	result, err := this.Db.HGet(key, field).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil
		}
		p_error.ThrowInternalError(`redis hget error`, err)
	}
	p_logger.Logger.Debug(fmt.Sprintf(`redis hget. key: %s, field: %s, value: %s`, key, field, result))
	return &result
}

func (this *_HashClass) HSet(key, field string, value interface{}) {
	p_logger.Logger.Debug(fmt.Sprintf(`redis hset. key: %s, field: %s, value: %s`, key, field, value))
	if err := this.Db.HSet(key, field, value).Err(); err != nil {
		p_error.ThrowInternalError(`redis hset error`, err)
	}
}
