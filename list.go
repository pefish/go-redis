package go_redis

import (
	"fmt"

	"github.com/go-redis/redis"
	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
)

type ListType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 将一个或多个值插入到列表头部
func (t *ListType) LPush(key string, value string) (listLength_ uint64, err_ error) {
	t.logger.Debug(fmt.Sprintf(`redis lpush. key: %s, val: %s`, key, value))
	len, err := t.db.LPush(key, value).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return uint64(len), nil
}

// 在列表中添加一个或多个值到列表尾部
func (lc *ListType) RPush(key string, value string) (listLength_ uint64, err_ error) {
	lc.logger.Debug(fmt.Sprintf(`redis rpush. key: %s, val: %s`, key, value))
	len, err := lc.db.RPush(key, value).Result()
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

// 获取列表指定范围内的元素
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
	return result, nil
}

// 获取列表中所有的元素
func (lc *ListType) ListAll(key string) ([]string, error) {
	result, err := lc.Range(key, 0, -1)
	if err != nil {
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
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
