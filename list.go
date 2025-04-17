package go_redis

import (
	"fmt"
	"strconv"

	"github.com/go-redis/redis"
	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
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
	len, err := t.db.LPush(key, valuesInterface...).Result()
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
	len, err := t.db.LPush(key, valuesInterface...).Result()
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
	len, err := t.db.RPush(key, valuesInterface...).Result()
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
	len, err := t.db.RPush(key, valuesInterface...).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return uint64(len), nil
}

// 移出并获取列表的第一个元素
func (lc *ListType) LPop(key string) (string, error) {
	lc.logger.Debug(fmt.Sprintf(`redis lpop. key: %s`, key))
	result, err := lc.db.LPop(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return "", nil
		}
		return "", errors.Wrapf(err, "<key: %s>", key)
	}
	lc.logger.Debug(fmt.Sprintf(`redis lpop. result: %s`, result))
	return result, nil
}

func (t *ListType) LPopUint64(key string) (uint64, error) {
	resultStr, err := t.LPop(key)
	if err != nil {
		return 0, err
	}
	r, err := strconv.ParseUint(resultStr, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s> string<%s> to uint64 failed.", key, resultStr)
	}
	return r, nil
}

// 移除列表的最后一个元素，返回值为移除的元素。
func (lc *ListType) RPop(key string) (string, error) {
	lc.logger.Debug(fmt.Sprintf(`redis rpop. key: %s`, key))
	result, err := lc.db.RPop(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return "", nil
		}
		return "", errors.Wrapf(err, "<key: %s>", key)
	}
	lc.logger.Debug(fmt.Sprintf(`redis rpop. result: %s`, result))
	return result, nil
}

func (t *ListType) RPopUint64(key string) (uint64, error) {
	resultStr, err := t.RPop(key)
	if err != nil {
		return 0, err
	}
	r, err := strconv.ParseUint(resultStr, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s> string<%s> to uint64 failed.", key, resultStr)
	}
	return r, nil
}

// 获取列表长度
func (lc *ListType) Len(key string) (uint64, error) {
	lc.logger.Debug(fmt.Sprintf(`redis llen. key: %s`, key))
	result, err := lc.db.LLen(key).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	lc.logger.Debug(fmt.Sprintf(`redis llen. result: %d`, result))
	return uint64(result), nil
}

// 获取列表指定范围内的元素，key 不存在返回 nil,nil
func (lc *ListType) Range(key string, start int64, stop int64) ([]string, error) {
	lc.logger.Debug(fmt.Sprintf(`redis lrange. key: %s, start: %d, stop: %d`, key, start, stop))
	result, err := lc.db.LRange(key, start, stop).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return []string{}, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	lc.logger.Debug(fmt.Sprintf(`redis lrange. result: #%v`, result))
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// 获取列表中所有的元素，key 不存在返回 nil,nil
func (lc *ListType) ListAll(key string) ([]string, error) {
	result, err := lc.Range(key, 0, -1)
	if err != nil {
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

// 获取列表中所有的元素，key 不存在返回 nil,nil
func (lc *ListType) ListAllUint64(key string) ([]uint64, error) {
	resultStrs, err := lc.Range(key, 0, -1)
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
func (lc *ListType) Get(key string, index int) (string, error) {
	lc.logger.DebugF(`redis lindex. key: %s`, key)
	result, err := lc.db.LIndex(key, int64(index)).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return "", nil
		}
		return "", errors.Wrapf(err, "<key: %s>", key)
	}
	lc.logger.DebugF(`redis lindex. result: %s`, result)
	return result, nil
}

// 根据索引获取列表中的元素，key 不存在时返回 0
func (lc *ListType) GetUint64(key string, index int) (uint64, error) {
	resultStr, err := lc.Get(key, index)
	if err != nil {
		return 0, err
	}
	if resultStr == "" {
		return 0, nil
	}
	r, err := strconv.ParseUint(resultStr, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s> <index: %d> string<%s> to uint64 failed.", key, index, resultStr)
	}
	return r, nil
}

// 根据索引设置列表中的元素，key 不存在时报错
func (lc *ListType) Set(key string, index int, value string) error {
	lc.logger.DebugF(`redis lset. key: %s`, key)
	_, err := lc.db.LSet(key, int64(index), value).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

func (lc *ListType) SetUint64(key string, index int, value uint64) error {
	err := lc.Set(key, index, strconv.FormatUint(value, 10))
	if err != nil {
		return err
	}
	return nil
}

// 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素(索引从左边开始)，不在指定区间之内的元素都将被删除。
func (lc *ListType) LTrim(key string, start int64, stop int64) error {
	lc.logger.Debug(fmt.Sprintf(`redis ltrim. key: %s, start: %d, stop: %d`, key, start, stop))
	_, err := lc.db.LTrim(key, start, stop).Result()
	if err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}
