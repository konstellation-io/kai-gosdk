package prediction

import (
	"context"
)

func (r *RedisPredictionStore) Delete(ctx context.Context, predictionID string) error {
	if predictionID == "" {
		return ErrInvalidPredictionID
	}

	result, err := r.client.Del(ctx, r.getKeyWithProductPrefix(predictionID)).Result()
	if err != nil {
		return err
	}

	if result == 0 {
		return ErrPredictionNotFound
	}

	return nil
}
