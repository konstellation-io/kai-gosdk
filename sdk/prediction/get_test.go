//go:build integration

package prediction_test

import (
	"context"
	"time"

	"github.com/konstellation-io/kai-gosdk/sdk/prediction"
)

func (s *PredictionStoreSuite) TestPredictionStore_Get_ExpectOK() {
	var (
		ctx                = context.Background()
		predictionID       = "test-prediction"
		expectedPrediction = &prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: map[string]interface{}{
				"test-key": "test-value",
			},
			Metadata: prediction.Metadata{},
		}
	)

	err := s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix(predictionID), "$", expectedPrediction).
		Err()
	s.Require().NoError(err)

	// WHEN
	actualPrediction, err := s.predictionStore.Get(ctx, predictionID)
	s.Require().NoError(err)

	// THEN
	s.Equal(expectedPrediction, actualPrediction)
}

func (s *PredictionStoreSuite) TestPredictionStore_Get_ProductWithoutPemissions() {
	var (
		ctx                    = context.Background()
		predictionID           = "test-prediction"
		otherProductPermission = &prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: map[string]interface{}{
				"test-key": "test-value",
			},
			Metadata: prediction.Metadata{},
		}
	)

	err := s.redisClient.
		JSONSet(ctx, "other-product:"+predictionID, "$", otherProductPermission).
		Err()
	s.Require().NoError(err)

	// WHEN
	actualPrediction, err := s.predictionStore.Get(ctx, predictionID)

	// THEN
	s.ErrorIs(err, prediction.ErrPredictionNotFound)
	s.Nil(actualPrediction)
}

func (s *PredictionStoreSuite) TestPredictionStore_Get_FailsInvalidKey() {
	ctx := context.Background()

	// WHEN
	actualPrediction, err := s.predictionStore.Get(ctx, "not-found-key")

	// THEN
	s.ErrorIs(err, prediction.ErrPredictionNotFound)
	s.Nil(actualPrediction)
}
