package go_redis

import (
	"context"
	"fmt"
	"strconv"

	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type ListType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 将一个或多个值插入到列表头部
func (t *ListType) LPush(key string, values ...string) (listLength_ uint64, err_ error) {
	t.logger.Debug(fmt.Sprintf(`redis lpush. key: %s, val: %#v`, key, values))
	valuesInterface := make([]any, 0)
	for _, v := range values {
		valuesInterface = append(valuesInterface, v)
	}
	len, err := t.db.LPush(context.Background(), key, valuesInterface...).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return uint64(len), nil
}

func (t *ListType) LPushUint64(key string, values ...uint64) (listLength_ uint64, err_ error) {
	t.logger.Debug(fmt.Sprintf(`redis lpush. key: %s, val: %#v`, key, values))
	valuesInterface := make([]any, 0)
	for _, v := range values {
		valuesInterface = append(valuesInterface, v)
	}
	len, err := t.db.LPush(context.Background(), key, valuesInterface...).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return uint64(len), nil
}

// 在列表中添加一个或多个值到列表尾部
func (t *ListType) RPush(key string, values ...string) (listLength_ uint64, err_ error) {
	t.logger.Debug(fmt.Sprintf(`redis rpush. key: %s, val: %#v`, key, values))
	valuesInterface := make([]any, 0)
	for _, v := range values {
		valuesInterface = append(valuesInterface, v)
	}
	len, err := t.db.RPush(context.Background(), key, valuesInterface...).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return uint64(len), nil
}

func (t *ListType) RPushUint64(key string, values ...uint64) (listLength_ uint64, err_ error) {
	t.logger.Debug(fmt.Sprintf(`redis rpush. key: %s, val: %#v`, key, values))
	valuesInterface := make([]any, 0)
	for _, v := range values {
		valuesInterface = append(valuesInterface, v)
	}
	len, err := t.db.RPush(context.Background(), key, valuesInterface...).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return uint64(len), nil
}

// 移出并获取列表的第一个元素
func (t *ListType) LPop(key string) (string, error) {
	t.logger.Debug(fmt.Sprintf(`redis lpop. key: %s`, key))
	result, err := t.db.LPop(context.Background(), key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return "", nil
		}
		return "", errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.Debug(fmt.Sprintf(`redis lpop. result: %s`, result))
	return result, nil
}

func (t *ListType) LPopUint64(key string) (uint64, error) {
	t.logger.Debug(fmt.Sprintf(`redis lpop. key: %s`, key))
	result, err := t.db.LPop(context.Background(), key).Uint64()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.Debug(fmt.Sprintf(`redis lpop. result: %d`, result))
	return result, nil
}

// 移除列表的最后一个元素，返回值为移除的元素。
func (t *ListType) RPop(key string) (string, error) {
	t.logger.Debug(fmt.Sprintf(`redis rpop. key: %s`, key))
	result, err := t.db.RPop(context.Background(), key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return "", nil
		}
		return "", errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.Debug(fmt.Sprintf(`redis rpop. result: %s`, result))
	return result, nil
}

func (t *ListType) RPopUint64(key string) (uint64, error) {
	t.logger.Debug(fmt.Sprintf(`redis rpop. key: %s`, key))
	result, err := t.db.RPop(context.Background(), key).Uint64()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.Debug(fmt.Sprintf(`redis rpop. result: %d`, result))
	return result, nil
}

// 获取列表长度
func (t *ListType) Len(key string) (uint64, error) {
	t.logger.Debug(fmt.Sprintf(`redis llen. key: %s`, key))
	result, err := t.db.LLen(context.Background(), key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.Debug(fmt.Sprintf(`redis llen. result: %d`, result))
	return uint64(result), nil
}

// 获取列表指定范围内的元素，key 不存在返回 nil,nil
func (t *ListType) Range(key string, start int64, stop int64) ([]string, error) {
	t.logger.Debug(fmt.Sprintf(`redis lrange. key: %s, start: %d, stop: %d`, key, start, stop))
	result, err := t.db.LRange(context.Background(), key, start, stop).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return []string{}, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.Debug(fmt.Sprintf(`redis lrange. result: #%v`, result))
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// 获取列表中所有的元素，key 不存在返回 nil,nil
func (t *ListType) ListAll(key string) ([]string, error) {
	result, err := t.Range(key, 0, -1)
	if err != nil {
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

// 获取列表中所有的元素，key 不存在返回 nil,nil
func (t *ListType) ListAllUint64(key string) ([]uint64, error) {
	resultStrs, err := t.Range(key, 0, -1)
	if err != nil {
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	result := make([]uint64, 0)
	for i, resultStr := range resultStrs {
		resultUint64, err := strconv.ParseUint(resultStr, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "<key: %s> <index: %d> string <%s> to uint64 failed.", key, i, resultStr)
		}
		result = append(result, resultUint64)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// 根据索引获取列表中的元素，key 不存在时返回空字符串
func (t *ListType) Get(key string, index int) (string, error) {
	t.logger.DebugF(`redis lindex. key: %s`, key)
	result, err := t.db.LIndex(context.Background(), key, int64(index)).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return "", nil
		}
		return "", errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.DebugF(`redis lindex. result: %s`, result)
	return result, nil
}

// 根据索引获取列表中的元素，key 不存在时返回 0
func (t *ListType) GetUint64(key string, index int) (uint64, error) {
	t.logger.DebugF(`redis lindex. key: %s`, key)
	result, err := t.db.LIndex(context.Background(), key, int64(index)).Uint64()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	t.logger.DebugF(`redis lindex. result: %s`, result)
	return result, nil
}

// 根据索引设置列表中的元素，key 不存在时报错
func (t *ListType) Set(key string, index int, value string) error {
	t.logger.DebugF(`redis lset. key: %s`, key)
	_, err := t.db.LSet(context.Background(), key, int64(index), value).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

func (t *ListType) SetUint64(key string, index int, value uint64) error {
	t.logger.DebugF(`redis lset. key: %s`, key)
	_, err := t.db.LSet(context.Background(), key, int64(index), value).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

// 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素(索引从左边开始)，不在指定区间之内的元素都将被删除。
func (t *ListType) LTrim(key string, start int64, stop int64) error {
	t.logger.Debug(fmt.Sprintf(`redis ltrim. key: %s, start: %d, stop: %d`, key, start, stop))
	_, err := t.db.LTrim(context.Background(), key, start, stop).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}
