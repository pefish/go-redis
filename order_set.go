package go_redis

import (
	"github.com/go-redis/redis"
	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
)

type OrderSetType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 向有序集合添加一个或多个成员，或者更新已存在成员的分数
func (t *OrderSetType) Add(key string, member string, score float64) error {
	t.logger.DebugF(`Redis zadd. key: %s, member: %s, score: %f`, key, member, score)
	if err := t.db.ZAdd(key, redis.Z{
		Score:  score,
		Member: member,
	}).Err(); err != nil {
		return errors.Wrap(err, "<key: %s>")
	}
	return nil
}

// 移除有序集合中的一个或多个成员
func (rc *OrderSetType) Remove(key string, member string) error {
	rc.logger.DebugF(`Redis ZRem. key: %s, member: %s`, key, member)
	if err := rc.db.ZRem(key, member).Err(); err != nil {
		return errors.Wrap(err, "<key: %s>")
	}
	return nil
}

// 移除有序集合中给定的分数区间的所有成员
func (rc *OrderSetType) RemRangeByScore(key string, min string, max string) error {
	rc.logger.DebugF(`Redis ZRemRangeByScore. key: %s, min: %s, max: %s`, key, min, max)
	if err := rc.db.ZRemRangeByScore(key, min, max).Err(); err != nil {
		return errors.Wrap(err, "<key: %s>")
	}
	return nil
}

func (rc *OrderSetType) Count(key string, min string, max string) (int64, error) {
	rc.logger.DebugF(`Redis Zcount. key: %s, min: %s, max: %s`, key, min, max)
	r, err := rc.db.ZCount(key, min, max).Result()
	if err != nil {
		return 0, errors.Wrap(err, "<key: %s>")
	}
	return r, nil
}

// 有序集合中对指定成员的分数加上增量 increment
func (rc *OrderSetType) IncrBy(key string, member string, increment float64) error {
	rc.logger.DebugF(`Redis ZIncrBy. key: %s, member: %s, increment: %f`, key, member, increment)
	if err := rc.db.ZIncrBy(key, increment, member).Err(); err != nil {
		return errors.Wrap(err, "<key: %s>")
	}
	return nil
}

// 返回有序集中，指定索引区间内的成员。其中成员的位置按分数值从小到大来排序. start 0, end -1 可取出全部
func (rc *OrderSetType) Range(key string, start int64, stop int64) ([]string, error) {
	rc.logger.DebugF(`Redis ZRange. key: %s, start: %s, stop: %s`, key, start, stop)
	result, err := rc.db.ZRange(key, start, stop).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrap(err, "<key: %s>")
	}
	return result, nil
}

// 返回有序集中，指定索引区间内的成员。其中成员的位置按分数值从大到小. start 0, end -1 可取出全部
func (rc *OrderSetType) RevRange(key string, start int64, stop int64) ([]string, error) {
	rc.logger.DebugF(`Redis ZRevRange. key: %s, start: %s, stop: %s`, key, start, stop)
	result, err := rc.db.ZRevRange(key, start, stop).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrap(err, "<key: %s>")
	}
	return result, nil
}

// 通过分数返回有序集合指定区间内(左右都包含)的成员, 分数从低到高排序
func (rc *OrderSetType) RangeByScore(key string, opt *redis.ZRangeBy) ([]string, error) {
	rc.logger.DebugF(`Redis ZRangeByScore. key: %s, min: %s, max: %s`, key, opt.Min, opt.Max)
	result, err := rc.db.ZRangeByScore(key, *opt).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrap(err, "<key: %s>")
	}
	return result, nil
}

// 返回有序集中指定分数区间内(左右都包含)的成员, 分数从高到低排序
func (rc *OrderSetType) RevRangeByScore(key string, opt *redis.ZRangeBy) ([]string, error) {
	rc.logger.DebugF(`Redis ZRevRangeByScore. key: %s, min: %s, max: %s`, key, opt.Min, opt.Max)
	result, err := rc.db.ZRevRangeByScore(key, *opt).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrap(err, "<key: %s>")
	}
	return result, nil
}

// 返回有序集中指定分数区间内的成员以及分数，分数从高到低排序
func (rc *OrderSetType) RevRangeByScoreWithScores(key string, opt *redis.ZRangeBy) ([]redis.Z, error) {
	rc.logger.DebugF(`Redis ZRevRangeByScoreWithScores. key: %s, min: %s, max: %s`, key, opt.Min, opt.Max)
	result, err := rc.db.ZRevRangeByScoreWithScores(key, *opt).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return nil, nil
		}
		return nil, errors.Wrap(err, "<key: %s>")
	}
	return result, nil
}

// 返回有序集中，成员的分数值
func (rc *OrderSetType) Score(key string, member string) (float64, error) {
	rc.logger.DebugF(`Redis ZScore. key: %s, member: %s`, key, member)
	result, err := rc.db.ZScore(key, member).Result()
	if err != nil {
		if err.Error() == `redis: nil` {
			return 0, nil
		}
		return 0, errors.Wrap(err, "<key: %s>")
	}
	return result, nil
}
