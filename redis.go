package go_redis

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis"
	i_logger "github.com/pefish/go-interface/i-logger"
)

var RedisInstance = NewRedisInstance(&i_logger.DefaultLogger)

// ----------------------------- RedisClass -----------------------------

type RedisType struct {
	Db     *redis.Client
	Set    *_SetType
	List   *_ListType
	String *_StringClass
	Order  *_OrderSetType
	Hash   *_HashType

	logger i_logger.ILogger
}

func NewRedisInstance(logger i_logger.ILogger) *RedisType {
	return &RedisType{
		logger: logger,
	}
}

type Configuration struct {
	Address  string
	Db       uint64
	Password string
}

func (rc *RedisType) Close() {
	if rc.Db != nil {
		err := rc.Db.Close()
		if err != nil {
			rc.logger.Error(err)
		} else {
			rc.logger.Info(`Redis close succeed.`)
		}
	}
}

func (rc *RedisType) Connect(configuration *Configuration) error {
	password := ``
	if configuration.Password != `` {
		password = configuration.Password
	}
	var database = configuration.Db

	if !strings.Contains(configuration.Address, ":") {
		configuration.Address += ":6379"
	}
	rc.logger.InfoF(`Redis connecting.... url: %s`, configuration.Address)
	rc.Db = redis.NewClient(&redis.Options{
		Addr:     configuration.Address,
		Password: password,
		DB:       int(database),
	})
	_, err := rc.Db.Ping().Result()
	if err != nil {
		return err
	}
	rc.logger.Info(`Redis connect succeed.`)

	rc.Set = &_SetType{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.List = &_ListType{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.String = &_StringClass{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.Order = &_OrderSetType{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.Hash = &_HashType{
		db:     rc.Db,
		logger: rc.logger,
	}
	return nil
}

func (rc *RedisType) Del(key string) error {
	rc.logger.DebugF(`Redis del. key: %s`, key)
	if err := rc.Db.Del(key).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *RedisType) Expire(key string, expiration time.Duration) error {
	rc.logger.DebugF(`Redis expire. key: %s, expiration: %v`, key, expiration)
	if err := rc.Db.Expire(key, expiration).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *RedisType) GetLock(key string, value string, expiration time.Duration) (bool, error) {
	result, err := rc.String.SetNx(key, value, expiration)
	if err != nil {
		return false, err
	}
	if result {
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

func (rc *RedisType) ReleaseLock(key string, value string) error {
	script := `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`
	rc.logger.DebugF(`Redis eval. script: %s`, script)
	result := rc.Db.Eval(script, []string{key}, []string{value})
	if err := result.Err(); err != nil {
		return err
	}
	return nil
}

// ----------------------------- _SetClass -----------------------------

type _SetType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 向集合添加一个或多个成员
func (rc *_SetType) Add(key string, member string) error {
	rc.logger.DebugF(`Redis sadd. key: %s, member: %s`, key, member)
	if err := rc.db.SAdd(key, member).Err(); err != nil {
		return err
	}
	return nil
}

// 返回集合中的所有成员
func (rc *_SetType) Members(key string) ([]string, error) {
	rc.logger.DebugF(`Redis smembers. key: %s`, key)
	result, err := rc.db.SMembers(key).Result()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// 判断 member 元素是否是集合 key 的成员
func (rc *_SetType) IsMember(key string, member string) (bool, error) {
	rc.logger.DebugF(`Redis sismember. key: %s, member: %s`, key, member)
	result, err := rc.db.SIsMember(key, member).Result()
	if err != nil {
		return false, err
	}
	return result, nil
}

// 移除集合中一个或多个成员
func (rc *_SetType) Remove(key string, members ...interface{}) error {
	rc.logger.DebugF(`Redis srem. key: %s, members: %v`, key, members)
	_, err := rc.db.SRem(key, members...).Result()
	if err != nil {
		return err
	}
	return nil
}

// ----------------------------- _ListClass -----------------------------

type _ListType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 将一个或多个值插入到列表头部
func (lc *_ListType) LPush(key string, value string) error {
	lc.logger.Debug(fmt.Sprintf(`redis lpush. key: %s, val: %s`, key, value))
	if err := lc.db.LPush(key, value).Err(); err != nil {
		return err
	}
	return nil
}

// 在列表中添加一个或多个值到列表尾部
func (lc *_ListType) RPush(key string, value string) error {
	lc.logger.Debug(fmt.Sprintf(`redis rpush. key: %s, val: %s`, key, value))
	if err := lc.db.RPush(key, value).Err(); err != nil {
		return err
	}
	return nil
}

// 移出并获取列表的第一个元素
func (lc *_ListType) LPop(key string) (string, error) {
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

// 移除列表的最后一个元素，返回值为移除的元素。
func (lc *_ListType) RPop(key string) (string, error) {
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

// 获取列表长度
func (lc *_ListType) Len(key string) (uint64, error) {
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

// 获取列表指定范围内的元素
func (lc *_ListType) Range(key string, start int64, stop int64) ([]string, error) {
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

// 获取列表中所有的元素
func (lc *_ListType) ListAll(key string) ([]string, error) {
	result, err := lc.Range(key, 0, -1)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
func (lc *_ListType) Trim(key string, start int64, stop int64) error {
	lc.logger.Debug(fmt.Sprintf(`redis ltrim. key: %s, start: %d, stop: %d`, key, start, stop))
	if err := lc.db.LTrim(key, start, stop).Err(); err != nil {
		return err
	}
	return nil
}

// ----------------------------- _StringClass -----------------------------

type _StringClass struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 设置指定 key 的值。
func (rc *_StringClass) Set(key string, value string, expiration time.Duration) error {
	rc.logger.DebugF(`Redis set. key: %s, val: %s, expiration: %v`, key, value, expiration)
	if err := rc.db.Set(key, value, expiration).Err(); err != nil {
		return err
	}
	return nil
}

// 只有在 key 不存在时设置 key 的值，设置成功返回 true。
func (rc *_StringClass) SetNx(key string, value string, expiration time.Duration) (bool, error) {
	rc.logger.DebugF(`Redis setnx. key: %s, val: %s, expiration: %v`, key, value, expiration)
	result := rc.db.SetNX(key, value, expiration)
	if err := result.Err(); err != nil {
		return false, err
	}
	return result.Val(), nil
}

// 获取指定 key 的值。
func (rc *_StringClass) Get(key string) (string, error) {
	rc.logger.DebugF(`Redis get. key: %s`, key)
	result, err := rc.db.Get(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, err
	}
	rc.logger.DebugF(`Redis get. result: %s`, result)
	return result, nil
}

// ----------------------------- _OrderSetClass -----------------------------

type _OrderSetType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 向有序集合添加一个或多个成员，或者更新已存在成员的分数
func (rc *_OrderSetType) Add(key string, member string, score float64) error {
	rc.logger.DebugF(`Redis zadd. key: %s, member: %s, score: %f`, key, member, score)
	if err := rc.db.ZAdd(key, redis.Z{
		Score:  score,
		Member: member,
	}).Err(); err != nil {
		return err
	}
	return nil
}

// 移除有序集合中的一个或多个成员
func (rc *_OrderSetType) Remove(key string, member string) error {
	rc.logger.DebugF(`Redis ZRem. key: %s, member: %s`, key, member)
	if err := rc.db.ZRem(key, member).Err(); err != nil {
		return err
	}
	return nil
}

// 移除有序集合中给定的分数区间的所有成员
func (rc *_OrderSetType) RemRangeByScore(key string, min string, max string) error {
	rc.logger.DebugF(`Redis ZRemRangeByScore. key: %s, min: %s, max: %s`, key, min, max)
	if err := rc.db.ZRemRangeByScore(key, min, max).Err(); err != nil {
		return err
	}
	return nil
}

// 有序集合中对指定成员的分数加上增量 increment
func (rc *_OrderSetType) IncrBy(key string, member string, increment float64) error {
	rc.logger.DebugF(`Redis ZIncrBy. key: %s, member: %s, increment: %f`, key, member, increment)
	if err := rc.db.ZIncrBy(key, increment, member).Err(); err != nil {
		return err
	}
	return nil
}

// 通过分数返回有序集合指定区间内的成员
func (rc *_OrderSetType) RangeByScore(key string, opt *redis.ZRangeBy) ([]string, error) {
	rc.logger.DebugF(`Redis ZRangeByScore. key: %s, min: %s, max: %s`, key, opt.Min, opt.Max)
	result, err := rc.db.ZRangeByScore(key, *opt).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, err
	}
	rc.logger.DebugF(`Redis ZRangeByScore. result: %+v`, result)
	return result, nil
}

// 返回有序集中指定分数区间内的成员，分数从高到低排序
func (rc *_OrderSetType) RevRangeByScore(key string, opt *redis.ZRangeBy) ([]string, error) {
	rc.logger.DebugF(`Redis ZRevRangeByScore. key: %s, min: %s, max: %s`, key, opt.Min, opt.Max)
	result, err := rc.db.ZRevRangeByScore(key, *opt).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, err
	}
	rc.logger.DebugF(`Redis ZRevRangeByScore. result: %+v`, result)
	return result, nil
}

// 返回有序集中指定分数区间内的成员以及分数，分数从高到低排序
func (rc *_OrderSetType) RevRangeByScoreWithScores(key string, opt *redis.ZRangeBy) ([]redis.Z, error) {
	rc.logger.DebugF(`Redis ZRevRangeByScoreWithScores. key: %s, min: %s, max: %s`, key, opt.Min, opt.Max)
	result, err := rc.db.ZRevRangeByScoreWithScores(key, *opt).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, err
	}
	rc.logger.DebugF(`Redis ZRevRangeByScoreWithScores. result: %+v`, result)
	return result, nil
}

// 返回有序集中，成员的分数值
func (rc *_OrderSetType) Score(key string, member string) (float64, error) {
	rc.logger.DebugF(`Redis ZScore. key: %s, member: %s`, key, member)
	result, err := rc.db.ZScore(key, member).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, err
	}
	rc.logger.DebugF(`Redis ZScore. result: %f`, result)
	return result, nil
}

// ----------------------------- _HashClass -----------------------------

type _HashType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 获取存储在哈希表中指定字段的值。
func (rc *_HashType) Get(key, field string) (string, error) {
	rc.logger.DebugF(`Redis hget. key: %s, field: %s`, key, field)
	result, err := rc.db.HGet(key, field).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, err
	}
	rc.logger.DebugF(`Redis hget. result: %s`, result)
	return result, nil
}

// 获取在哈希表中指定 key 的所有字段和值
func (rc *_HashType) GetAll(key string) (map[string]string, error) {
	rc.logger.DebugF(`Redis hgetall. key: %s`, key)
	result, err := rc.db.HGetAll(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return map[string]string{}, nil
		}
		return nil, err
	}
	rc.logger.DebugF(`Redis hgetall. result: %s`, result)
	return result, nil
}

// 将哈希表 key 中的字段 field 的值设为 value 。
func (rc *_HashType) Set(key, field string, value interface{}) error {
	rc.logger.DebugF(`Redis hset. key: %s, field: %s, value: %s`, key, field, value)
	if err := rc.db.HSet(key, field, value).Err(); err != nil {
		return err
	}
	return nil
}
