package prediction

import (
	"context"
	"errors"
	"time"
)

var (
	ErrEmptyPayload        = errors.New("empty payload")
	ErrInvalidPredictionID = errors.New("invalid prediction ID")
)

func (r *RedisPredictionStore) Save(ctx context.Context, predictionID string, payload Payload) error {
	if predictionID == "" {
		return ErrInvalidPredictionID
	}

	if payload == nil {
		return ErrEmptyPayload
	}

	prediction := Prediction{
		CreationDate: time.Now().UnixMilli(),
		LastModified: time.Now().UnixMilli(),
		Payload:      payload,
		Metadata: Metadata{
			Product:      r.metadata.GetProduct(),
			Version:      r.metadata.GetVersion(),
			Workflow:     r.metadata.GetWorkflow(),
			WorkflowType: r.metadata.GetWorkflowType(),
			Process:      r.metadata.GetProcess(),
			RequestID:    r.requestID,
		},
	}

	keyWithProductPrefix := r.getKeyWithProductPrefix(predictionID)

	return r.client.JSONSet(ctx, keyWithProductPrefix, "$", prediction).Err()
}
