package go_redis

import (
	"fmt"
	"github.com/go-redis/redis"
	go_logger "github.com/pefish/go-logger"
	"strings"
	"time"
)

var RedisInstance = NewRedisInstance()

// ----------------------------- RedisClass -----------------------------

type RedisClass struct {
	Db     *redis.Client
	Set    *_SetClass
	List   *_ListClass
	String *_StringClass
	Order  *_OrderSetClass
	Hash   *_HashClass

	logger go_logger.InterfaceLogger
}

func NewRedisInstance() *RedisClass {
	return &RedisClass{
		logger: go_logger.Logger,
	}
}

type Configuration struct {
	Address  string
	Db       uint64
	Password string
}

func (rc *RedisClass) Close() {
	if rc.Db != nil {
		err := rc.Db.Close()
		if err != nil {
			rc.logger.Error(err)
		} else {
			rc.logger.Info(`redis close succeed.`)
		}
	}
}

func (rc *RedisClass) SetLogger(logger go_logger.InterfaceLogger) *RedisClass {
	rc.logger = logger
	return rc
}

func (rc *RedisClass) MustConnect(configuration Configuration) {
	err := rc.Connect(configuration)
	if err != nil {
		panic(err)
	}
}

func (rc *RedisClass) Connect(configuration Configuration) error {
	password := ``
	if configuration.Password != `` {
		password = configuration.Password
	}
	var database = configuration.Db

	if strings.Index(configuration.Address, ":") == -1 {
		configuration.Address += ":6379"
	}
	rc.logger.Info(fmt.Sprintf(`redis connecting.... url: %s`, configuration.Address))
	rc.Db = redis.NewClient(&redis.Options{
		Addr:     configuration.Address,
		Password: password,
		DB:       int(database),
	})
	_, err := rc.Db.Ping().Result()
	if err != nil {
		return err
	}
	rc.logger.Info(fmt.Sprintf(`redis connect succeed.`))

	rc.Set = &_SetClass{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.List = &_ListClass{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.String = &_StringClass{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.Order = &_OrderSetClass{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.Hash = &_HashClass{
		db:     rc.Db,
		logger: rc.logger,
	}
	return nil
}

func (rc *RedisClass) MustDel(key string) {
	err := rc.Del(key)
	if err != nil {
		panic(err)
	}
}

func (rc *RedisClass) Del(key string) error {
	rc.logger.Debug(fmt.Sprintf(`redis del. key: %s`, key))
	if err := rc.Db.Del(key).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *RedisClass) MustExpire(key string, expiration time.Duration) {
	err := rc.Expire(key, expiration)
	if err != nil {
		panic(err)
	}
}

func (rc *RedisClass) Expire(key string, expiration time.Duration) error {
	rc.logger.Debug(fmt.Sprintf(`redis expire. key: %s, expiration: %v`, key, expiration))
	if err := rc.Db.Expire(key, expiration).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *RedisClass) MustGetLock(key string, value string, expiration time.Duration) bool {
	result, err := rc.GetLock(key, value, expiration)
	if err != nil {
		panic(err)
	}
	return result
}

func (rc *RedisClass) GetLock(key string, value string, expiration time.Duration) (bool, error) {
	result, err := rc.String.SetNx(key, value, expiration)
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
				v, _ := rc.String.Get(key)
				if v == value {
					err := rc.Expire(key, expiration)
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

func (rc *RedisClass) MustReleaseLock(key string, value string) {
	err := rc.ReleaseLock(key, value)
	if err != nil {
		panic(err)
	}
}

func (rc *RedisClass) ReleaseLock(key string, value string) error {
	script := `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`
	rc.logger.Debug(fmt.Sprintf(`redis eval. script: %s`, script))
	result := rc.Db.Eval(script, []string{key}, []string{value})
	if err := result.Err(); err != nil {
		return err
	}
	return nil
}

// ----------------------------- _SetClass -----------------------------

type _SetClass struct {
	db     *redis.Client
	logger go_logger.InterfaceLogger
}

func (rc *_SetClass) MustSadd(key string, value string) {
	err := rc.Sadd(key, value)
	if err != nil {
		panic(err)
	}
}

func (rc *_SetClass) Sadd(key string, value string) error {
	rc.logger.Debug(fmt.Sprintf(`redis sadd. key: %s, value: %s`, key, value))
	if err := rc.db.SAdd(key, value).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *_SetClass) MustSmembers(key string) []string {
	result, err := rc.Smembers(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (rc *_SetClass) Smembers(key string) ([]string, error) {
	rc.logger.Debug(fmt.Sprintf(`redis smembers. key: %s`, key))
	result, err := rc.db.SMembers(key).Result()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (rc *_SetClass) MustSisMember(key string, member string) bool {
	result, err := rc.SisMember(key, member)
	if err != nil {
		panic(err)
	}
	return result
}

func (rc *_SetClass) SisMember(key string, member string) (bool, error) {
	rc.logger.Debug(fmt.Sprintf(`redis sismember. key: %s, member: %s`, key, member))
	result, err := rc.db.SIsMember(key, member).Result()
	if err != nil {
		return false, err
	}
	return result, nil
}

func (rc *_SetClass) MustSrem(key string, members ...interface{}) {
	err := rc.Srem(key, members)
	if err != nil {
		panic(err)
	}
}

func (rc *_SetClass) Srem(key string, members ...interface{}) error {
	rc.logger.Debug(fmt.Sprintf(`redis srem. key: %s, members: %v`, key, members))
	_, err := rc.db.SRem(key, members...).Result()
	if err != nil {
		return err
	}
	return nil
}

// ----------------------------- _ListClass -----------------------------

type _ListClass struct {
	db     *redis.Client
	logger go_logger.InterfaceLogger
}

func (lc *_ListClass) MustLPush(key string, value string) {
	err := lc.LPush(key, value)
	if err != nil {
		panic(err)
	}
}

func (lc *_ListClass) LPush(key string, value string) error {
	lc.logger.Debug(fmt.Sprintf(`redis lpush. key: %s, val: %s`, key, value))
	if err := lc.db.LPush(key, value).Err(); err != nil {
		return err
	}
	return nil
}

func (lc *_ListClass) MustRPush(key string, value string) {
	err := lc.RPush(key, value)
	if err != nil {
		panic(err)
	}
}

func (lc *_ListClass) RPush(key string, value string) error {
	lc.logger.Debug(fmt.Sprintf(`redis rpush. key: %s, val: %s`, key, value))
	if err := lc.db.RPush(key, value).Err(); err != nil {
		return err
	}
	return nil
}

func (lc *_ListClass) MustLPop(key string) string {
	result, err := lc.LPop(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (lc *_ListClass) LPop(key string) (string, error) {
	lc.logger.Debug(fmt.Sprintf(`redis lpop. key: %s`, key))
	result, err := lc.db.LPop(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return "", nil
		}
		return "", err
	}
	lc.logger.Debug(fmt.Sprintf(`redis lpop. result: %s`, result))
	return result, nil
}

func (lc *_ListClass) MustRPop(key string) string {
	result, err := lc.RPop(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (lc *_ListClass) RPop(key string) (string, error) {
	lc.logger.Debug(fmt.Sprintf(`redis rpop. key: %s`, key))
	result, err := lc.db.RPop(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return "", nil
		}
		return "", err
	}
	lc.logger.Debug(fmt.Sprintf(`redis rpop. result: %s`, result))
	return result, nil
}

func (lc *_ListClass) MustLLen(key string) uint64 {
	result, err := lc.LLen(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (lc *_ListClass) LLen(key string) (uint64, error) {
	lc.logger.Debug(fmt.Sprintf(`redis llen. key: %s`, key))
	result, err := lc.db.LLen(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, err
	}
	lc.logger.Debug(fmt.Sprintf(`redis llen. result: %d`, result))
	return uint64(result), nil
}

func (lc *_ListClass) MustLRange(key string, start int64, stop int64) []string {
	result, err := lc.LRange(key, start, stop)
	if err != nil {
		panic(err)
	}
	return result
}

func (lc *_ListClass) LRange(key string, start int64, stop int64) ([]string, error) {
	lc.logger.Debug(fmt.Sprintf(`redis lrange. key: %s, start: %d, stop: %d`, key, start, stop))
	result, err := lc.db.LRange(key, start, stop).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return []string{}, nil
		}
		return nil, err
	}
	lc.logger.Debug(fmt.Sprintf(`redis lrange. result: #%v`, result))
	return result, nil
}

func (lc *_ListClass) MustListAll(key string) []string {
	result, err := lc.ListAll(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (lc *_ListClass) ListAll(key string) ([]string, error) {
	result, err := lc.LRange(key, 0, -1)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (lc *_ListClass) MustLTrim(key string, start int64, stop int64) {
	err := lc.LTrim(key, start, stop)
	if err != nil {
		panic(err)
	}
}

func (lc *_ListClass) LTrim(key string, start int64, stop int64) error {
	lc.logger.Debug(fmt.Sprintf(`redis ltrim. key: %s, start: %d, stop: %d`, key, start, stop))
	if err := lc.db.LTrim(key, start, stop).Err(); err != nil {
		return err
	}
	return nil
}

// ----------------------------- _StringClass -----------------------------

type _StringClass struct {
	db     *redis.Client
	logger go_logger.InterfaceLogger
}

func (rc *_StringClass) MustSet(key string, value string, expiration time.Duration) {
	err := rc.Set(key, value, expiration)
	if err != nil {
		panic(err)
	}
}

func (rc *_StringClass) Set(key string, value string, expiration time.Duration) error {
	rc.logger.Debug(fmt.Sprintf(`redis set. key: %s, val: %s, expiration: %v`, key, value, expiration))
	if err := rc.db.Set(key, value, expiration).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *_StringClass) MustSetNx(key string, value string, expiration time.Duration) bool {
	result, err := rc.SetNx(key, value, expiration)
	if err != nil {
		panic(err)
	}
	return result
}

/*
*
设置成功返回true
*/
func (rc *_StringClass) SetNx(key string, value string, expiration time.Duration) (bool, error) {
	rc.logger.Debug(fmt.Sprintf(`redis setnx. key: %s, val: %s, expiration: %v`, key, value, expiration))
	result := rc.db.SetNX(key, value, expiration)
	if err := result.Err(); err != nil {
		return false, err
	}
	return result.Val(), nil
}

func (rc *_StringClass) MustGet(key string) string {
	result, err := rc.Get(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (rc *_StringClass) Get(key string) (string, error) {
	rc.logger.Debug(fmt.Sprintf(`redis get. key: %s`, key))
	result, err := rc.db.Get(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, err
	}
	rc.logger.Debug(fmt.Sprintf(`redis get. result: %s`, result))
	return result, nil
}

// ----------------------------- _OrderSetClass -----------------------------

type _OrderSetClass struct {
	db     *redis.Client
	logger go_logger.InterfaceLogger
}

// ----------------------------- _HashClass -----------------------------

type _HashClass struct {
	db     *redis.Client
	logger go_logger.InterfaceLogger
}

func (rc *_HashClass) MustHmget(key string, field string) string {
	result, err := rc.Hmget(key, field)
	if err != nil {
		panic(err)
	}
	return result
}

func (rc *_HashClass) Hmget(key string, field string) (string, error) {
	rc.logger.Debug(fmt.Sprintf(`redis hmget. key: %s, field: %s`, key, field))
	val, err := rc.db.HMGet(key, field).Result()
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
	rc.logger.Debug(fmt.Sprintf(`redis hmget. result: %s`, result))
	return result, nil
}

func (rc *_HashClass) MustHGet(key, field string) string {
	result, err := rc.HGet(key, field)
	if err != nil {
		panic(err)
	}
	return result
}

func (rc *_HashClass) HGet(key, field string) (string, error) {
	rc.logger.Debug(fmt.Sprintf(`redis hget. key: %s, field: %s`, key, field))
	result, err := rc.db.HGet(key, field).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, err
	}
	rc.logger.Debug(fmt.Sprintf(`redis hget. result: %s`, result))
	return result, nil
}

func (rc *_HashClass) MustHGetAll(key string) map[string]string {
	result, err := rc.HGetAll(key)
	if err != nil {
		panic(err)
	}
	return result
}

func (rc *_HashClass) HGetAll(key string) (map[string]string, error) {
	rc.logger.Debug(fmt.Sprintf(`redis hgetall. key: %s`, key))
	result, err := rc.db.HGetAll(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return map[string]string{}, nil
		}
		return nil, err
	}
	rc.logger.Debug(fmt.Sprintf(`redis hgetall. result: %s`, result))
	return result, nil
}

func (rc *_HashClass) MustHSet(key, field string, value interface{}) {
	err := rc.HSet(key, field, value)
	if err != nil {
		panic(err)
	}
}

func (rc *_HashClass) HSet(key, field string, value interface{}) error {
	rc.logger.Debug(fmt.Sprintf(`redis hset. key: %s, field: %s, value: %s`, key, field, value))
	if err := rc.db.HSet(key, field, value).Err(); err != nil {
		return err
	}
	return nil
}
