package go_redis

import (
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

type InterfaceLogger interface {
	Debug(args ...interface{})
	DebugF(format string, args ...interface{})
	Info(args ...interface{})
	InfoF(format string, args ...interface{})
	Warn(args ...interface{})
	WarnF(format string, args ...interface{})
	Error(args ...interface{})
	ErrorF(format string, args ...interface{})
}

var RedisHelper = RedisClass{}

// ----------------------------- RedisClass -----------------------------

type RedisClass struct {
	Db     *redis.Client
	Set    *_SetClass
	List   *_ListClass
	String *_StringClass
	Order  *_OrderSetClass
	Hash   *_HashClass

	logger InterfaceLogger
}

type Configuration struct {
	Host     string
	Port     uint64
	Db       uint64
	Password string
}

func (this *RedisClass) Close() {
	if this.Db != nil {
		err := this.Db.Close()
		if err != nil {
			this.logger.Error(err)
		} else {
			this.logger.Info(`redis close succeed.`)
		}
	}
}

func (this *RedisClass) SetLogger(logger InterfaceLogger) {
	this.logger = logger
}

func (this *RedisClass) MustConnectWithMap(map_ map[string]interface{}) {
	var port uint64 = 6379
	if map_[`port`] != nil {
		port = uint64(map_[`port`].(float64))
	}
	password := ``
	if map_[`password`] != nil {
		password = map_[`password`].(string)
	}
	var database uint64 = 0
	if map_[`db`] != nil {
		database = uint64(map_[`db`].(float64))
	}
	this.MustConnect(map_[`host`].(string), port, password, database)
}

func (this *RedisClass) MustConnectWithConfiguration(configuration Configuration) {
	var port uint64 = 6379
	if configuration.Port != 0 {
		port = configuration.Port
	}
	password := ``
	if configuration.Password != `` {
		password = configuration.Password
	}
	var database = configuration.Db
	this.MustConnect(configuration.Host, port, password, database)
}

func (this *RedisClass) MustConnect(host string, port uint64, password string, database uint64) {
	err := this.Connect(host, port, password, database)
	if err != nil {
		panic(err)
	}
}

func (this *RedisClass) Connect(host string, port uint64, password string, database uint64) error {
	address := fmt.Sprintf(`%s:%d`, host, port)
	this.Db = redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       int(database),
	})
	_, err := this.Db.Ping().Result()
	if err != nil {
		return err
	}
	this.logger.Info(fmt.Sprintf(`redis connect succeed. url: %s`, address))

	this.Set = &_SetClass{
		db:     this.Db,
		logger: this.logger,
	}
	this.List = &_ListClass{
		db:     this.Db,
		logger: this.logger,
	}
	this.String = &_StringClass{
		db:     this.Db,
		logger: this.logger,
	}
	this.Order = &_OrderSetClass{
		db:     this.Db,
		logger: this.logger,
	}
	this.Hash = &_HashClass{
		db:     this.Db,
		logger: this.logger,
	}
	return nil
}

func (this *RedisClass) MustDel(key string) {
	err := this.Del(key)
	if err != nil {
		panic(err)
	}
}

func (this *RedisClass) Del(key string) error {
	this.logger.Debug(fmt.Sprintf(`redis del. key: %s`, key))
	if err := this.Db.Del(key).Err(); err != nil {
		return err
	}
	return nil
}

func (this *RedisClass) MustExpire(key string, expiration time.Duration) {
	err := this.Expire(key, expiration)
	if err != nil {
		panic(err)
	}
}

func (this *RedisClass) Expire(key string, expiration time.Duration) error {
	this.logger.Debug(fmt.Sprintf(`redis expire. key: %s, expiration: %v`, key, expiration))
	if err := this.Db.Expire(key, expiration).Err(); err != nil {
		return err
	}
	return nil
}

func (this *RedisClass) MustGetLock(key string, value string, expiration time.Duration) bool {
	result, err := this.GetLock(key, value, expiration)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *RedisClass) GetLock(key string, value string, expiration time.Duration) (bool, error) {
	result, err := this.String.SetNx(key, value, expiration)
	if err != nil {
		return false, err
	}
	if result == true {
		// 自动续锁
		go func() {
			timerInterval := expiration / 2
			d := time.Duration(timerInterval)
			t := time.NewTicker(d)
			defer t.Stop()

			for {
				<-t.C
				v, _ := this.String.Get(key)
				if v == value {
					err := this.Expire(key, expiration)
					if err != nil {
						break
					}
				} else {
					break
				}
			}
		}()
	}
	return result, nil
}

func (this *RedisClass) MustReleaseLock(key string, value string) {
	err := this.ReleaseLock(key, value)
	if err != nil {
		panic(err)
	}
}

func (this *RedisClass) ReleaseLock(key string, value string) error {
	script := `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`
	this.logger.Debug(fmt.Sprintf(`redis eval. script: %s`, script))
	result := this.Db.Eval(script, []string{key}, []string{value})
	if err := result.Err(); err != nil {
		return err
	}
	return nil
}

// ----------------------------- _SetClass -----------------------------

type _SetClass struct {
	db     *redis.Client
	logger InterfaceLogger
}

func (this *_SetClass) MustSadd(key string, value string) {
	err := this.Sadd(key, value)
	if err != nil {
		panic(err)
	}
}

func (this *_SetClass) Sadd(key string, value string) error {
	this.logger.Debug(fmt.Sprintf(`redis sadd. key: %s, value: %s`, key, value))
	if err := this.db.SAdd(key, value).Err(); err != nil {
		return err
	}
	return nil
}

func (this *_SetClass) MustSmembers(key string) []string {
	result, err := this.Smembers(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *_SetClass) Smembers(key string) ([]string, error) {
	this.logger.Debug(fmt.Sprintf(`redis smembers. key: %s`, key))
	result, err := this.db.SMembers(key).Result()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (this *_SetClass) MustSisMember(key string, member string) bool {
	result, err := this.SisMember(key, member)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *_SetClass) SisMember(key string, member string) (bool, error) {
	this.logger.Debug(fmt.Sprintf(`redis sismember. key: %s, member: %s`, key, member))
	result, err := this.db.SIsMember(key, member).Result()
	if err != nil {
		return false, err
	}
	return result, nil
}

func (this *_SetClass) MustSrem(key string, members ...interface{}) {
	err := this.Srem(key, members)
	if err != nil {
		panic(err)
	}
}

func (this *_SetClass) Srem(key string, members ...interface{}) error {
	this.logger.Debug(fmt.Sprintf(`redis srem. key: %s, members: %v`, key, members))
	_, err := this.db.SRem(key, members...).Result()
	if err != nil {
		return err
	}
	return nil
}

// ----------------------------- _ListClass -----------------------------

type _ListClass struct {
	db     *redis.Client
	logger InterfaceLogger
}

// ----------------------------- _StringClass -----------------------------

type _StringClass struct {
	db     *redis.Client
	logger InterfaceLogger
}

func (this *_StringClass) MustSet(key string, value string, expiration time.Duration) {
	err := this.Set(key, value, expiration)
	if err != nil {
		panic(err)
	}
}

func (this *_StringClass) Set(key string, value string, expiration time.Duration) error {
	this.logger.Debug(fmt.Sprintf(`redis set. key: %s, val: %s, expiration: %v`, key, value, expiration))
	if err := this.db.Set(key, value, expiration).Err(); err != nil {
		return err
	}
	return nil
}

func (this *_StringClass) MustSetNx(key string, value string, expiration time.Duration) bool {
	result, err := this.SetNx(key, value, expiration)
	if err != nil {
		panic(err)
	}
	return result
}

/**
设置成功返回true
*/
func (this *_StringClass) SetNx(key string, value string, expiration time.Duration) (bool, error) {
	this.logger.Debug(fmt.Sprintf(`redis setnx. key: %s, val: %s, expiration: %v`, key, value, expiration))
	result := this.db.SetNX(key, value, expiration)
	if err := result.Err(); err != nil {
		return false, err
	}
	return result.Val(), nil
}

func (this *_StringClass) MustGet(key string) string {
	result, err := this.Get(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *_StringClass) Get(key string) (string, error) {
	this.logger.Debug(fmt.Sprintf(`redis get. key: %s`, key))
	result, err := this.db.Get(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, err
	}
	this.logger.Debug(fmt.Sprintf(`redis get. result: %s`, result))
	return result, nil
}

// ----------------------------- _OrderSetClass -----------------------------

type _OrderSetClass struct {
	db     *redis.Client
	logger InterfaceLogger
}

// ----------------------------- _HashClass -----------------------------

type _HashClass struct {
	db     *redis.Client
	logger InterfaceLogger
}

func (this *_HashClass) MustHmget(key string, field string) string {
	result, err := this.Hmget(key, field)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *_HashClass) Hmget(key string, field string) (string, error) {
	this.logger.Debug(fmt.Sprintf(`redis hmget. key: %s, field: %s`, key, field))
	val, err := this.db.HMGet(key, field).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, err
	}
	if len(val) == 0 || val[0] == nil {
		return ``, nil
	}
	result := val[0].(string)
	this.logger.Debug(fmt.Sprintf(`redis hmget. result: %s`, result))
	return result, nil
}

func (this *_HashClass) MustHGet(key, field string) string {
	result, err := this.HGet(key, field)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *_HashClass) HGet(key, field string) (string, error) {
	this.logger.Debug(fmt.Sprintf(`redis hget. key: %s, field: %s`, key, field))
	result, err := this.db.HGet(key, field).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, err
	}
	this.logger.Debug(fmt.Sprintf(`redis hget. result: %s`, result))
	return result, nil
}


func (this *_HashClass) MustHGetAll(key string) map[string]string {
	result, err := this.HGetAll(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *_HashClass) HGetAll(key string) (map[string]string, error) {
	this.logger.Debug(fmt.Sprintf(`redis hgetall. key: %s`, key))
	result, err := this.db.HGetAll(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return map[string]string{}, nil
		}
		return nil, err
	}
	this.logger.Debug(fmt.Sprintf(`redis hgetall. result: %s`, result))
	return result, nil
}


func (this *_HashClass) MustHSet(key, field string, value interface{}) {
	err := this.HSet(key, field, value)
	if err != nil {
		panic(err)
	}
}

func (this *_HashClass) HSet(key, field string, value interface{}) error {
	this.logger.Debug(fmt.Sprintf(`redis hset. key: %s, field: %s, value: %s`, key, field, value))
	if err := this.db.HSet(key, field, value).Err(); err != nil {
		return err
	}
	return nil
}
