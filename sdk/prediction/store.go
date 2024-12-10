package prediction

import (
	"errors"
	"fmt"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/metadata"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

var (
	ErrPredictionNotFound = errors.New("prediction not found")
	ErrParsingListResult  = errors.New("error parsing prediction store search result")
)

type RedisPredictionStore struct {
	requestID string
	client    *redis.Client
	metadata  *metadata.Metadata
}

func NewRedisPredictionStore(requestID string) *RedisPredictionStore {
	opts := &redis.Options{
		Addr:     viper.GetString(common.ConfigRedisEndpointKey),
		Username: viper.GetString(common.ConfigRedisUsernameKey),
		Password: viper.GetString(common.ConfigRedisPasswordKey),
	}

	return &RedisPredictionStore{
		client:    redis.NewClient(opts),
		metadata:  metadata.New(),
		requestID: requestID,
	}
}

func (r *RedisPredictionStore) getKeyWithProductPrefix(key string) string {
	return fmt.Sprintf("%s:%s", r.metadata.GetProduct(), key)
}
