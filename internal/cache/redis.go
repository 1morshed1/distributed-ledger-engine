package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// RedisCache implements BalanceCache backed by Redis.
type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(addr string) *RedisCache {
	return &RedisCache{
		client: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}

// key namespaces balance entries so this DB can hold other cache data later.
func key(accountID int64) string {
	return fmt.Sprintf("balance:%d", accountID)
}

func (c *RedisCache) GetBalance(ctx context.Context, accountID int64) (int64, bool, error) {
	val, err := c.client.Get(ctx, key(accountID)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil // cache miss, not an error
	}
	if err != nil {
		return 0, false, err
	}
	balance, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		// Corrupt value: treat as miss so the caller falls back to Postgres.
		return 0, false, nil
	}
	return balance, true, nil
}

func (c *RedisCache) SetBalance(ctx context.Context, accountID int64, balance int64) error {
	return c.client.Set(ctx, key(accountID), balance, defaultTTL).Err()
}

func (c *RedisCache) Invalidate(ctx context.Context, accountID int64) error {
	return c.client.Del(ctx, key(accountID)).Err()
}
