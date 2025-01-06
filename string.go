package go_redis

import (
	"strconv"
	"time"

	"github.com/go-redis/redis"
	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
)

type StringType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 设置指定 key 的值。
func (t *StringType) Set(key string, value string, expiration time.Duration) error {
	t.logger.DebugF(`Redis set. key: %s, val: %s, expiration: %v`, key, value, expiration)
	if err := t.db.Set(key, value, expiration).Err(); err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

func (t *StringType) SetUint64(key string, value uint64, expiration time.Duration) error {
	return t.Set(key, strconv.FormatUint(value, 10), expiration)
}

// 只有在 key 不存在时设置 key 的值，设置成功返回 true。
func (t *StringType) SetNx(key string, value string, expiration time.Duration) (bool, error) {
	t.logger.DebugF(`Redis setnx. key: %s, val: %s, expiration: %v`, key, value, expiration)
	result := t.db.SetNX(key, value, expiration)
	if err := result.Err(); err != nil {
		return false, errors.Wrapf(err, "<key: %s>", key)
	}
	return result.Val(), nil
}

// 获取指定 key 的值。
func (t *StringType) Get(key string) (string, error) {
	t.logger.DebugF(`Redis get. key: %s`, key)
	result, err := t.db.Get(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.DebugF(`Redis get. result: %s`, result)
	return result, nil
}

func (t *StringType) GetUint64(key string) (uint64, error) {
	resultStr, err := t.Get(key)
	if err != nil {
		return 0, err
	}
	if resultStr == "" {
		return 0, nil
	}
	r, err := strconv.ParseUint(resultStr, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s> string to uint64 failed.", key)
	}
	return r, nil
}

func (t *StringType) GetFloat64(key string) (float64, error) {
	resultStr, err := t.Get(key)
	if err != nil {
		return 0, err
	}
	if resultStr == "" {
		return 0, nil
	}
	r, err := strconv.ParseFloat(resultStr, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s> string to float64 failed.", key)
	}
	return r, nil
}
