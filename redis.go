package p_redis

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/pefish/go-application"
	"github.com/pefish/go-error"
	"github.com/pefish/go-logger"
	"github.com/pefish/go-reflect"
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
	this.Order = &_OrderClass{this.Db}
	this.Hash = &_HashClass{this.Db}
}

func (this *RedisClass) Del(key string) {
	if p_application.Application.Debug {
		p_logger.Logger.Debug(fmt.Sprintf(`redis del. key: %s`, key))
	}
	_, err := this.Db.Del(key).Result()
	if err != nil {
		p_error.ThrowInternalError(`redis del error`, err)
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
		p_error.ThrowInternalError(`redis sadd error`, err)
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
		p_error.ThrowInternalError(`redis set error`, err)
	}
}

func (this *_StringClass) Get(key string) *string {
	result, err := this.Db.Get(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil
		}
		p_error.ThrowInternalError(`redis get error`, err)
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
		p_error.ThrowInternalError(`redis hmget error`, err)
	}
	if len(val) == 0 || val[0] == nil {
		return nil
	}
	result := val[0].(string)
	return &result
}
