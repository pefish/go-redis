package go_redis

import (
	"time"

	"github.com/go-redis/redis"
	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
)

type StringClass struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 设置指定 key 的值。
func (t *StringClass) Set(key string, value string, expiration time.Duration) error {
	t.logger.DebugF(`Redis set. key: %s, val: %s, expiration: %v`, key, value, expiration)
	if err := t.db.Set(key, value, expiration).Err(); err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

// 只有在 key 不存在时设置 key 的值，设置成功返回 true。
func (rc *StringClass) SetNx(key string, value string, expiration time.Duration) (bool, error) {
	rc.logger.DebugF(`Redis setnx. key: %s, val: %s, expiration: %v`, key, value, expiration)
	result := rc.db.SetNX(key, value, expiration)
	if err := result.Err(); err != nil {
		return false, errors.Wrapf(err, "<key: %s>", key)
	}
	return result.Val(), nil
}

// 获取指定 key 的值。
func (rc *StringClass) Get(key string) (string, error) {
	rc.logger.DebugF(`Redis get. key: %s`, key)
	result, err := rc.db.Get(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, errors.Wrapf(err, "<key: %s>", key)
	}
	rc.logger.DebugF(`Redis get. result: %s`, result)
	return result, nil
}
