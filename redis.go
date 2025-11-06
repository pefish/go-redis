package go_redis

import (
	"context"
	"strings"
	"time"
	"unsafe"

	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// ----------------------------- RedisClass -----------------------------

type RedisType struct {
	Db       *redis.Client
	Set      *SetType
	List     *ListType
	String   *StringType
	OrderSet *OrderSetType
	Hash     *HashType

	logger  i_logger.ILogger
	timeout time.Duration
}

type StringOrBytes interface {
	~string | ~[]byte
}

func NewRedisInstance(logger i_logger.ILogger, timeout time.Duration) *RedisType {
	return &RedisType{
		logger:  logger,
		timeout: timeout,
	}
}

type Configuration struct {
	Url      string
	Db       uint64
	Password string
}

func (t *RedisType) Close() {
	if t.Db != nil {
		err := t.Db.Close()
		if err != nil {
			t.logger.Error(err)
		} else {
			t.logger.Info(`Redis close succeed.`)
		}
	}
}

func (t *RedisType) Connect(configuration *Configuration) error {
	password := ``
	if configuration.Password != `` {
		password = configuration.Password
	}
	var database = configuration.Db

	if !strings.Contains(configuration.Url, ":") {
		configuration.Url += ":6379"
	}
	t.logger.InfoF(`Redis connecting.... url: %s`, configuration.Url)
	t.Db = redis.NewClient(&redis.Options{
		Addr:     configuration.Url,
		Password: password,
		DB:       int(database),
	}).WithTimeout(t.timeout)
	_, err := t.Db.Ping(context.Background()).Result()
	if err != nil {
		return errors.Wrap(err, "")
	}
	t.logger.Info(`Redis connect succeed.`)

	t.Set = &SetType{
		db:     t.Db,
		logger: t.logger,
	}
	t.List = &ListType{
		db:     t.Db,
		logger: t.logger,
	}
	t.String = &StringType{
		db:     t.Db,
		logger: t.logger,
	}
	t.OrderSet = &OrderSetType{
		db:     t.Db,
		logger: t.logger,
	}
	t.Hash = &HashType{
		db:     t.Db,
		logger: t.logger,
	}
	return nil
}

func (rc *RedisType) Del(key string) (bool, error) {
	rc.logger.DebugF(`Redis del. key: %s`, key)
	result := rc.Db.Del(context.Background(), key)
	if result.Err() != nil {
		return false, errors.Wrapf(result.Err(), "<key: %s>", key)
	}
	return result.Val() == 1, nil
}

func (rc *RedisType) Exists(key string) (bool, error) {
	rc.logger.DebugF(`Redis exists. key: %s`, key)
	result, err := rc.Db.Exists(context.Background(), key).Result()
	if err != nil {
		return false, errors.Wrapf(err, "<key: %s>", key)
	}
	return result == 1, nil
}

func (rc *RedisType) Keys(pattern string) ([]string, error) {
	rc.logger.DebugF(`Redis keys. pattern: %s`, pattern)
	results, err := rc.Db.Keys(context.Background(), pattern).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "<pattern: %s>", pattern)
	}
	return results, nil
}

func (rc *RedisType) Publish(channel string, message string) (receivedSubscriberCount_ uint64, err_ error) {
	rc.logger.DebugF(`Redis publish. channel: %s, message: %s`, channel, message)
	result, err := rc.Db.Publish(context.Background(), channel, message).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<channel: %s>", channel)
	}
	return uint64(result), nil
}

func (rc *RedisType) Subscribe(channel string) <-chan *redis.Message {
	rc.logger.DebugF(`Redis subscribe. channel: %s`, channel)
	return rc.Db.Subscribe(context.Background(), channel).Channel()
}

func (rc *RedisType) Expire(key string, expiration time.Duration) error {
	rc.logger.DebugF(`Redis expire. key: %s, expiration: %v`, key, expiration)
	if err := rc.Db.Expire(context.Background(), key, expiration).Err(); err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

func (rc *RedisType) GetLock(key string, value string, expiration time.Duration) (bool, error) {
	result, err := rc.String.SetNX(key, value, expiration)
	if err != nil {
		return false, errors.Wrapf(err, "<key: %s>", key)
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
	result := rc.Db.Eval(context.Background(), script, []string{key}, []string{value})
	if err := result.Err(); err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

// BytesToString converts byte slice to string.
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes converts string to byte slice.
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
