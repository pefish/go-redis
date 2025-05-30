package go_redis

import (
	"context"
	"strconv"

	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type HashType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

func (t *HashType) Exists(key, field string) (bool, error) {
	t.logger.DebugF(`Redis hexists. key: %s, field: %s`, key, field)
	result, err := t.db.HExists(context.Background(), key, field).Result()
	if err != nil {
		return false, errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.DebugF(`Redis hexists. result: %s`, result)
	return result, nil
}

// 获取存储在哈希表中指定字段的值。不存在就返回空字符串
func (t *HashType) Get(key, field string) (string, error) {
	t.logger.DebugF(`Redis hget. key: %s, field: %s`, key, field)
	result, err := t.db.HGet(context.Background(), key, field).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return ``, nil
		}
		return ``, errors.Wrapf(err, "<key: %s, field: %s>", key, field)
	}
	t.logger.DebugF(`Redis hget. result: %s`, result)
	return result, nil
}

func (t *HashType) RandomGetFields(key string, count int) ([]string, error) {
	t.logger.DebugF(`Redis HRandField. key: %s, count: %d`, key, count)
	result, err := t.db.HRandField(context.Background(), key, count).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.DebugF(`Redis HRandField. result: %s`, result)
	return result, nil
}

// 如果 key/field 不存在或者内容是空字符串，都返回 0
func (t *HashType) GetUint64(key, field string) (uint64, error) {
	resultStr, err := t.Get(key, field)
	if err != nil {
		return 0, err
	}
	if resultStr == "" {
		return 0, nil
	}
	r, err := strconv.ParseUint(resultStr, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s, field: %s> string<%s> to uint64 failed.", key, field, resultStr)
	}
	return r, nil
}

func (t *HashType) GetFloat64(key, field string) (float64, error) {
	resultStr, err := t.Get(key, field)
	if err != nil {
		return 0, err
	}
	if resultStr == "" {
		return 0, nil
	}

	r, err := strconv.ParseFloat(resultStr, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s, field: %s> string to float64 failed.", key, field)
	}
	return r, nil
}

// 获取在哈希表中指定 key 的所有字段和值
func (rc *HashType) GetAll(key string) (map[string]string, error) {
	rc.logger.DebugF(`Redis hgetall. key: %s`, key)
	result, err := rc.db.HGetAll(context.Background(), key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return map[string]string{}, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	rc.logger.DebugF(`Redis hgetall. result: %s`, result)
	return result, nil
}

// 将哈希表 key 中的字段 field 的值设为 value 。
func (rc *HashType) Set(key, field, value string) error {
	rc.logger.DebugF(`Redis hset. key: %s, field: %s, value: %s`, key, field, value)
	_, err := rc.db.HSet(context.Background(), key, field, value).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s, field: %s>", key, field)
	}
	return nil
}

func (rc *HashType) SetMulti(key string, datas map[string]string) error {
	rc.logger.DebugF(`Redis hmset. key: %s, datas: %#v`, key, datas)
	datasInterface := make(map[string]any, 0)
	for k, v := range datas {
		datasInterface[k] = v
	}
	_, err := rc.db.HMSet(context.Background(), key, datasInterface).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s, datas: %#v>", key, datas)
	}
	return nil
}

func (t *HashType) SetUint64(key, field string, value uint64) error {
	return t.Set(key, field, strconv.FormatUint(value, 10))
}

func (rc *HashType) SetNX(key, field string, value string) (bool, error) {
	rc.logger.DebugF(`Redis hsetnx. key: %s, field: %s, value: %s`, key, field, value)
	result, err := rc.db.HSetNX(context.Background(), key, field, value).Result()
	if err != nil {
		return false, errors.Wrapf(err, "<key: %s, field: %s>", key, field)
	}
	return result, nil
}

func (rc *HashType) Del(key, field string) error {
	rc.logger.DebugF(`Redis hdel. key: %s, field: %s`, key, field)
	err := rc.db.HDel(context.Background(), key, field).Err()
	if err != nil {
		return errors.Wrapf(err, "<key: %s, field: %s>", key, field)
	}
	return nil
}

func (rc *HashType) Len(key string) (int64, error) {
	rc.logger.DebugF(`Redis hlen. key: %s`, key)
	result, err := rc.db.HLen(context.Background(), key).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

func (rc *HashType) Fields(key string) ([]string, error) {
	rc.logger.DebugF(`Redis keys. key: %s`, key)
	result, err := rc.db.HKeys(context.Background(), key).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

func (rc *HashType) Values(key string) ([]string, error) {
	rc.logger.DebugF(`Redis hvals. key: %s`, key)
	result, err := rc.db.HVals(context.Background(), key).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}
