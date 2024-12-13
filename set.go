package go_redis

import (
	"github.com/go-redis/redis"
	i_logger "github.com/pefish/go-interface/i-logger"
	"github.com/pkg/errors"
)

type SetType struct {
	db     *redis.Client
	logger i_logger.ILogger
}

// 向集合添加一个或多个成员
func (t *SetType) Add(key string, member string) error {
	t.logger.DebugF(`Redis sadd. key: %s, member: %s`, key, member)
	if err := t.db.SAdd(key, member).Err(); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

// 返回集合中的所有成员
func (rc *SetType) Members(key string) ([]string, error) {
	rc.logger.DebugF(`Redis smembers. key: %s`, key)
	result, err := rc.db.SMembers(key).Result()
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return result, nil
}

// 判断 member 元素是否是集合 key 的成员
func (rc *SetType) IsMember(key string, member string) (bool, error) {
	rc.logger.DebugF(`Redis sismember. key: %s, member: %s`, key, member)
	result, err := rc.db.SIsMember(key, member).Result()
	if err != nil {
		return false, errors.Wrap(err, "")
	}
	return result, nil
}

// 移除集合中一个或多个成员
func (rc *SetType) Remove(key string, members ...string) error {
	rc.logger.DebugF(`Redis srem. key: %s, members: %v`, key, members)
	rawMembers := make([]interface{}, 0)
	for _, member := range members {
		rawMembers = append(rawMembers, member)
	}
	_, err := rc.db.SRem(key, rawMembers...).Result()
	if err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}
