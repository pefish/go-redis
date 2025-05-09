package go_redis

import (
	"context"
	"math"
	"strconv"

	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type OrderSetType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

type RangeBy struct {
	Min, Max      float64 // math.MaxFloat64 表示 +inf，-1 表示 -inf
	Offset, Count int64
}

// 向有序集合添加一个或多个成员，或者更新已存在成员的分数
func (t *OrderSetType) Add(key string, member string, score float64) error {
	t.logger.DebugF(`Redis zadd. key: %s, member: %s, score: %f`, key, member, score)
	if err := t.db.ZAdd(context.Background(), key, redis.Z{
		Score:  score,
		Member: member,
	}).Err(); err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

// 移除有序集合中的一个或多个成员
func (rc *OrderSetType) Remove(key string, member string) error {
	rc.logger.DebugF(`Redis ZRem. key: %s, member: %s`, key, member)
	if err := rc.db.ZRem(context.Background(), key, member).Err(); err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

// 移除有序集合中给定的分数区间的所有成员，math.MaxFloat64 表示 +inf，-1 表示 -inf
func (rc *OrderSetType) RemRangeByScore(key string, min float64, max float64) error {
	minStr := strconv.FormatFloat(min, 'f', -1, 64)
	if min == -1 {
		minStr = "-inf"
	}
	maxStr := strconv.FormatFloat(max, 'f', -1, 64)
	if max == math.MaxFloat64 {
		maxStr = "+inf"
	}
	rc.logger.DebugF(`Redis ZRemRangeByScore. key: %s, min: %s, max: %s`, key, minStr, maxStr)
	if err := rc.db.ZRemRangeByScore(context.Background(), key, minStr, maxStr).Err(); err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

// 统计分数范围内的元素个数
func (rc *OrderSetType) Count(key string, min float64, max float64) (int64, error) {
	minStr := strconv.FormatFloat(min, 'f', -1, 64)
	if min == -1 {
		minStr = "-inf"
	}
	maxStr := strconv.FormatFloat(max, 'f', -1, 64)
	if max == math.MaxFloat64 {
		maxStr = "+inf"
	}
	rc.logger.DebugF(`Redis Zcount. key: %s, min: %s, max: %s`, key, minStr, maxStr)
	r, err := rc.db.ZCount(context.Background(), key, minStr, maxStr).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return r, nil
}

// 得到元素总个数
func (rc *OrderSetType) TotalCount(key string) (int64, error) {
	rc.logger.DebugF(`Redis ZCard. key: %s`, key)
	r, err := rc.db.ZCard(context.Background(), key).Result()
	if err != nil {
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return r, nil
}

// 有序集合中对指定成员的分数加上增量 increment
func (rc *OrderSetType) IncrBy(key string, member string, increment float64) error {
	rc.logger.DebugF(`Redis ZIncrBy. key: %s, member: %s, increment: %f`, key, member, increment)
	if err := rc.db.ZIncrBy(context.Background(), key, increment, member).Err(); err != nil {
		return errors.Wrapf(err, "<key: %s>", key)
	}
	return nil
}

// 返回有序集中，指定索引区间内的成员。其中成员的位置按分数值从小到大来排序. start 0, end -1 可取出全部
func (rc *OrderSetType) Range(key string, start int64, stop int64) ([]string, error) {
	rc.logger.DebugF(`Redis ZRange. key: %s, start: %s, stop: %s`, key, start, stop)
	result, err := rc.db.ZRange(context.Background(), key, start, stop).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

// 返回有序集中，指定索引区间内的成员。其中成员的位置按分数值从大到小. start 0, end -1 可取出全部
func (rc *OrderSetType) RevRange(key string, start int64, stop int64) ([]string, error) {
	rc.logger.DebugF(`Redis ZRevRange. key: %s, start: %s, stop: %s`, key, start, stop)
	result, err := rc.db.ZRevRange(context.Background(), key, start, stop).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

// 通过分数返回有序集合指定区间内(左右都包含)的成员, 分数从低到高排序
func (rc *OrderSetType) RangeByScore(key string, rangeBy RangeBy) ([]string, error) {
	minStr := strconv.FormatFloat(rangeBy.Min, 'f', -1, 64)
	if rangeBy.Min == -1 {
		minStr = "-inf"
	}
	maxStr := strconv.FormatFloat(rangeBy.Max, 'f', -1, 64)
	if rangeBy.Max == math.MaxFloat64 {
		maxStr = "+inf"
	}

	rc.logger.DebugF(`Redis ZRangeByScore. key: %s, min: %f, max: %f`, key, minStr, maxStr)
	result, err := rc.db.ZRangeByScore(context.Background(), key, &redis.ZRangeBy{
		Min:    minStr,
		Max:    maxStr,
		Offset: rangeBy.Offset,
		Count:  rangeBy.Count,
	}).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

// 返回有序集中指定分数区间内(左右都包含)的成员, 分数从高到低排序
func (rc *OrderSetType) RevRangeByScore(key string, rangeBy RangeBy) ([]string, error) {
	minStr := strconv.FormatFloat(rangeBy.Min, 'f', -1, 64)
	if rangeBy.Min == -1 {
		minStr = "-inf"
	}
	maxStr := strconv.FormatFloat(rangeBy.Max, 'f', -1, 64)
	if rangeBy.Max == math.MaxFloat64 {
		maxStr = "+inf"
	}

	rc.logger.DebugF(`Redis ZRevRangeByScore. key: %s, min: %f, max: %f`, key, minStr, maxStr)
	result, err := rc.db.ZRevRangeByScore(context.Background(), key, &redis.ZRangeBy{
		Min:    minStr,
		Max:    maxStr,
		Offset: rangeBy.Offset,
		Count:  rangeBy.Count,
	}).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

// 返回有序集中指定分数区间内的成员以及分数，分数从高到低排序
func (rc *OrderSetType) RevRangeByScoreWithScores(key string, rangeBy RangeBy) ([]redis.Z, error) {
	minStr := strconv.FormatFloat(rangeBy.Min, 'f', -1, 64)
	if rangeBy.Min == -1 {
		minStr = "-inf"
	}
	maxStr := strconv.FormatFloat(rangeBy.Max, 'f', -1, 64)
	if rangeBy.Max == math.MaxFloat64 {
		maxStr = "+inf"
	}
	rc.logger.DebugF(`Redis ZRevRangeByScoreWithScores. key: %s, min: %f, max: %f`, key, minStr, maxStr)
	result, err := rc.db.ZRevRangeByScoreWithScores(context.Background(), key, &redis.ZRangeBy{
		Min:    minStr,
		Max:    maxStr,
		Offset: rangeBy.Offset,
		Count:  rangeBy.Count,
	}).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}

// 返回有序集中，成员的分数值
func (rc *OrderSetType) Score(key string, member string) (float64, error) {
	rc.logger.DebugF(`Redis ZScore. key: %s, member: %s`, key, member)
	result, err := rc.db.ZScore(context.Background(), key, member).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, errors.Wrapf(err, "<key: %s>", key)
	}
	return result, nil
}
