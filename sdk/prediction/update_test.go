//go:build integration

package prediction_test

import (
	"context"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/prediction"
)

func (s *PredictionStoreSuite) TestPredictionStore_Update_ExpectOK() {
	var (
		ctx          = context.Background()
		predict      = newPredictionBuilder().Build()
		predictionID = "test-prediction"
		newPayload   = prediction.Payload{
			"new-key": "new-value",
		}

		updatePayload = func(_ prediction.Payload) prediction.Payload {
			return newPayload
		}
	)

	err := s.redisClient.JSONSet(ctx, s.getKeyWithProductPrefix(predictionID), "$", predict.Payload).Err()
	s.Require().NoError(err)

	err = s.predictionStore.Update(ctx, predictionID, updatePayload)
	s.Require().NoError(err)

	updatedPrediction, err := s.predictionStore.Get(ctx, predictionID)
	s.Require().NoError(err)

	s.Equal(newPayload, updatedPrediction.Payload)
}

func (s *PredictionStoreSuite) TestPredictionStore_Update_FailsIfPredictionDoesntExist() {
	var (
		ctx          = context.Background()
		predictionID = "test-prediction"
		newPayload   = prediction.Payload{
			"new-key": "new-value",
		}

		updatePayload = func(_ prediction.Payload) prediction.Payload {
			return newPayload
		}
	)

	err := s.predictionStore.Update(ctx, predictionID, updatePayload)
	s.Require().ErrorIs(err, prediction.ErrPredictionNotFound)
}

func (s *PredictionStoreSuite) TestPredictionStore_Update_FailsIfUpdatedPayloadIsNil() {
	var (
		ctx          = context.Background()
		predict      = newPredictionBuilder().Build()
		predictionID = "test-prediction"

		updatePayload = func(_ prediction.Payload) prediction.Payload {
			return nil
		}
	)

	err := s.redisClient.JSONSet(ctx, s.getKeyWithProductPrefix(predictionID), "$", predict.Payload).Err()
	s.Require().NoError(err)

	err = s.predictionStore.Update(ctx, predictionID, updatePayload)
	s.ErrorIs(err, prediction.ErrEmptyPayload)
}
