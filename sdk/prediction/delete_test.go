//go:build integration

package prediction_test

import (
	"context"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/prediction"
)

func (s *PredictionStoreSuite) TestPredictionStore_Delete_ExpectOK() {
	var (
		ctx              = context.Background()
		predictionID     = "test-prediction"
		predictionValues = prediction.Payload{
			"test-key": "test-value",
		}
	)
	// WHEN
	err := s.predictionStore.Save(ctx, predictionID, predictionValues)
	s.Require().NoError(err)

	err = s.predictionStore.Delete(ctx, predictionID)
	s.Require().NoError(err)
}

func (s *PredictionStoreSuite) TestPredictionStore_Delete_FailsIfPredictionNotFound() {
	var (
		ctx          = context.Background()
		predictionID = "test-prediction"
	)
	// WHEN
	err := s.predictionStore.Delete(ctx, predictionID)

	// THEN
	s.Assert().ErrorIs(err, prediction.ErrPredictionNotFound)
}

func (s *PredictionStoreSuite) TestPredictionStore_Delete_InvalidPredictionID() {
	var (
		ctx          = context.Background()
		predictionID = ""
	)
	// WHEN
	err := s.predictionStore.Delete(ctx, predictionID)

	// THEN
	s.Assert().ErrorIs(err, prediction.ErrInvalidPredictionID)
}
