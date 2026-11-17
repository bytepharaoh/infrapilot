package receiver

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisQuota implements a weighted token bucket in Redis. The key contains the next
// virtual arrival time. Redis clock and atomic script execution make all replicas share it.
type RedisQuota struct {
	client     *redis.Client
	intervalUS int
	burst      int
}

var chargeScript = redis.NewScript(`
local now = redis.call("TIME")
local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
local interval = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local points = tonumber(ARGV[3])
local previous = tonumber(redis.call("GET", KEYS[1])) or now_us
local next = math.max(previous, now_us) + points * interval
if next > now_us + burst * interval then
  return 0
end
local ttl_ms = math.max(1000, math.ceil(burst * interval / 1000))
redis.call("PSETEX", KEYS[1], ttl_ms, string.format("%.0f", next))
return 1
`)

// NewRedisQuota sets the steady rate and burst in points for each organization.
func NewRedisQuota(client *redis.Client, pointsPerSecond, burst int) *RedisQuota {
	// Round up to an integer interval for a conservative rate and exact Redis arithmetic.
	intervalUS := (int(time.Second/time.Microsecond) + pointsPerSecond - 1) / pointsPerSecond
	return &RedisQuota{client: client, intervalUS: intervalUS, burst: burst}
}

// Charge debits an organization atomically, returning false when its bucket cannot pay.
func (q *RedisQuota) Charge(ctx context.Context, org uuid.UUID, points int) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	allowed, err := chargeScript.Run(ctx, q.client, []string{"infrapilot:ingest:quota:" + org.String()}, q.intervalUS, q.burst, points).Int()
	if err != nil {
		return false, fmt.Errorf("redis ingestion quota: %w", err)
	}
	return allowed == 1, nil
}
