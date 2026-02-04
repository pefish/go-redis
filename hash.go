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

func (t *HashType) GetBatch(key string, fields []string) ([]any, error) {
	t.logger.DebugF(`Redis hmget. key: %s, fields: ...`, key)
	result, err := t.db.HMGet(context.Background(), key, fields...).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "<key: %s, fields: ...>", key)
	}
	t.logger.DebugF(`Redis hmget. result: %s`, result)
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
	t.logger.DebugF(`Redis hget. key: %s, field: %s`, key, field)
	result, err := t.db.HGet(context.Background(), key, field).Uint64()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, errors.Wrapf(err, "<key: %s, field: %s>", key, field)
	}
	t.logger.DebugF(`Redis hget. result: %s`, result)
	return result, nil
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
func (t *HashType) GetAll(key string) (map[string]string, error) {
	t.logger.DebugF(`Redis hgetall. key: %s`, key)
	result, err := t.db.HGetAll(context.Background(), key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return map[string]string{}, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.DebugF(`Redis hgetall. result: %s`, result)
	return result, nil
}

// 将哈希表 key 中的字段 field 的值设为 value 。
func (t *HashType) Set(key, field, value string) error {
	t.logger.DebugF(`Redis hset. key: %s, field: %s, value: %s`, key, field, value)
	_, err := t.db.HSet(context.Background(), key, field, value).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s, field: %s>", key, field)
	}
	return nil
}

func (t *HashType) SetBatch(key string, fieldValues map[string]any) error {
	t.logger.DebugF(`Redis hset. key: %s, fieldValues: ...`, key)
	_, err := t.db.HSet(context.Background(), key, fieldValues).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

func (t *HashType) SetUint64(key, field string, value uint64) error {
	t.logger.DebugF(`Redis hset. key: %s, field: %s, value: %s`, key, field, value)
	_, err := t.db.HSet(context.Background(), key, field, value).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s, field: %s>", key, field)
	}
	return nil
}

func (t *HashType) SetNX(key, field string, value string) (bool, error) {
	t.logger.DebugF(`Redis hsetnx. key: %s, field: %s, value: %s`, key, field, value)
	result, err := t.db.HSetNX(context.Background(), key, field, value).Result()
	if err != nil {
		return false, errors.Wrapf(err, "<key: %s, field: %s>", key, field)
	}
	return result, nil
}

func (t *HashType) Del(key, field string) (bool, error) {
	t.logger.DebugF(`Redis hdel. key: %s, field: %s`, key, field)
	result := t.db.HDel(context.Background(), key, field)
	if result.Err() != nil {
		return false, errors.Wrapf(result.Err(), "<key: %s, field: %s>", key, field)
	}
	return result.Val() == 1, nil
}

func (t *HashType) Len(key string) (int64, error) {
	t.logger.DebugF(`Redis hlen. key: %s`, key)
	result, err := t.db.HLen(context.Background(), key).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

func (t *HashType) Fields(key string) ([]string, error) {
	t.logger.DebugF(`Redis keys. key: %s`, key)
	result, err := t.db.HKeys(context.Background(), key).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

func (t *HashType) Values(key string) ([]string, error) {
	t.logger.DebugF(`Redis hvals. key: %s`, key)
	result, err := t.db.HVals(context.Background(), key).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

func (t *HashType) IncrBy(key string, field string, increment int64) (int64, error) {
	t.logger.DebugF(`Redis HIncrBy. key: %s, field: %s, increment: %f`, key, field, increment)
	result := t.db.HIncrBy(context.Background(), key, field, increment)
	if err := result.Err(); err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}

	return result.Val(), nil
}
