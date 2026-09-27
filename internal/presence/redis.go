package presence

import (
	"context"
	"fmt"
	"strconv"
	"time"

	redis "github.com/redis/go-redis/v9"
)

const staleAfter = 30 * time.Second
const keyTTL = 60 * time.Second

// RedisStore is a soft routing hint. PostgreSQL remains the source of truth
// for messages; missing or stale presence never deletes a pending message.
type RedisStore struct {
	client *redis.Client
	now    func() time.Time
}

func New(addr, password string) *RedisStore {
	return &RedisStore{
		client: redis.NewClient(&redis.Options{Addr: addr, Password: password}),
		now:    time.Now,
	}
}

func (s *RedisStore) Close() error { return s.client.Close() }

func (s *RedisStore) Ping(ctx context.Context) error { return s.client.Ping(ctx).Err() }

func RouteKey(userID int64) string { return fmt.Sprintf("im:route:%d", userID) }

func (s *RedisStore) Touch(ctx context.Context, userID int64, instanceID string) error {
	return s.TouchMany(ctx, []int64{userID}, instanceID)
}

func (s *RedisStore) TouchMany(ctx context.Context, userIDs []int64, instanceID string) error {
	for start := 0; start < len(userIDs); start += 128 {
		end := min(start+128, len(userIDs))
		pipe := s.client.TxPipeline()
		for _, userID := range userIDs[start:end] {
			key := RouteKey(userID)
			pipe.ZAdd(ctx, key, redis.Z{Score: float64(s.now().UnixMilli()), Member: instanceID})
			pipe.Expire(ctx, key, keyTTL)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisStore) Remove(ctx context.Context, userID int64, instanceID string) error {
	return s.client.ZRem(ctx, RouteKey(userID), instanceID).Err()
}

func (s *RedisStore) Instances(ctx context.Context, userID int64) ([]string, error) {
	key := RouteKey(userID)
	cutoff := s.now().Add(-staleAfter).UnixMilli()
	if err := s.client.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(cutoff-1, 10)).Err(); err != nil {
		return nil, err
	}
	return s.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: strconv.FormatInt(cutoff, 10), Max: "+inf",
	}).Result()
}
