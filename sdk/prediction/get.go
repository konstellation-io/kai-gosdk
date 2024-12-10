package prediction

import (
	"context"
	"encoding/json"
	"fmt"
)

func (r *RedisPredictionStore) Get(ctx context.Context, predictionID string) (*Prediction, error) {
	result, err := r.client.JSONGet(ctx, r.getKeyWithProductPrefix(predictionID), "$").Result()
	if err != nil {
		return nil, err
	}

	if result == "" {
		return nil, ErrPredictionNotFound
	}

	var predictions []*Prediction
	if err := json.Unmarshal([]byte(result), &predictions); err != nil {
		return nil, fmt.Errorf("parsing prediction: %w", err)
	}

	return predictions[0], nil
}
