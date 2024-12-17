package go_redis

import (
	"strings"
	"time"
	"unsafe"

	"github.com/go-redis/redis"
	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
)

// ----------------------------- RedisClass -----------------------------

type RedisType struct {
	Db       *redis.Client
	Set      *SetType
	List     *ListType
	String   *StringClass
	OrderSet *OrderSetType
	Hash     *HashType

	logger i_logger.ILogger
}

type StringOrBytes interface {
	~string | ~[]byte
}

func NewRedisInstance(logger i_logger.ILogger) *RedisType {
	return &RedisType{
		logger: logger,
	}
}

type Configuration struct {
	Url      string
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

	if !strings.Contains(configuration.Url, ":") {
		configuration.Url += ":6379"
	}
	rc.logger.InfoF(`Redis connecting.... url: %s`, configuration.Url)
	rc.Db = redis.NewClient(&redis.Options{
		Addr:     configuration.Url,
		Password: password,
		DB:       int(database),
	})
	_, err := rc.Db.Ping().Result()
	if err != nil {
		return errors.Wrap(err, "")
	}
	rc.logger.Info(`Redis connect succeed.`)

	rc.Set = &SetType{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.List = &ListType{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.String = &StringClass{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.OrderSet = &OrderSetType{
		db:     rc.Db,
		logger: rc.logger,
	}
	rc.Hash = &HashType{
		db:     rc.Db,
		logger: rc.logger,
	}
	return nil
}

func (rc *RedisType) Del(key string) error {
	rc.logger.DebugF(`Redis del. key: %s`, key)
	if err := rc.Db.Del(key).Err(); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (rc *RedisType) Exists(key string) (bool, error) {
	rc.logger.DebugF(`Redis exists. key: %s`, key)
	result, err := rc.Db.Exists(key).Result()
	if err != nil {
		return false, errors.Wrap(err, "")
	}
	return result == 1, nil
}

func (rc *RedisType) Publish(channel string, message string) (receivedSubscriberCount_ uint64, err_ error) {
	rc.logger.DebugF(`Redis publish. channel: %s, message: %s`, channel, message)
	result, err := rc.Db.Publish(channel, message).Result()
	if err != nil {
		return 0, errors.Wrap(err, "")
	}
	return uint64(result), nil
}

func (rc *RedisType) Subscribe(channel string) <-chan *redis.Message {
	rc.logger.DebugF(`Redis subscribe. channel: %s`, channel)
	return rc.Db.Subscribe(channel).Channel()
}

func (rc *RedisType) Expire(key string, expiration time.Duration) error {
	rc.logger.DebugF(`Redis expire. key: %s, expiration: %v`, key, expiration)
	if err := rc.Db.Expire(key, expiration).Err(); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (rc *RedisType) GetLock(key string, value string, expiration time.Duration) (bool, error) {
	result, err := rc.String.SetNx(key, value, expiration)
	if err != nil {
		return false, errors.Wrap(err, "")
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
		return errors.Wrap(err, "")
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
