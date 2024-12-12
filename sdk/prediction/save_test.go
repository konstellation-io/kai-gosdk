//go:build integration

package prediction_test

import (
	"context"
	"encoding/json"

	"github.com/konstellation-io/kai-gosdk/sdk/prediction"
)

func (s *PredictionStoreSuite) TestPredictionStore_Save_ExpectOK() {
	var (
		ctx              = context.Background()
		predictionID     = "test-prediction"
		predictionValues = prediction.Payload{
			"test-key": "test-value",
		}
		expectedMetadata = s.getPredictionMetadata()
	)
	// WHEN
	err := s.predictionStore.Save(ctx, predictionID, predictionValues)
	s.Require().NoError(err)

	res, err := s.redisClient.JSONGet(ctx, s.getKeyWithProductPrefix(predictionID)).Result()
	s.Require().NoError(err)

	var actualPrediction []prediction.Prediction
	err = json.Unmarshal([]byte(res), &actualPrediction)
	s.Require().NoError(err)

	// THEN
	s.Equal(predictionValues, actualPrediction[0].Payload)
	s.Equal(expectedMetadata, actualPrediction[0].Metadata)
}

func (s *PredictionStoreSuite) TestPredictionStore_Save_FailsIfPayloadIsNil() {
	var (
		ctx          = context.Background()
		predictionID = "test-prediction"
	)
	// WHEN
	err := s.predictionStore.Save(ctx, predictionID, nil)

	// THEN
	s.Assert().ErrorIs(err, prediction.ErrEmptyPayload)
}

func (s *PredictionStoreSuite) TestPredictionStore_Save_InvalidPredictionID() {
	var (
		ctx          = context.Background()
		predictionID = ""
	)
	// WHEN
	err := s.predictionStore.Save(ctx, predictionID, prediction.Payload{"test": "test"})

	// THEN
	s.Assert().ErrorIs(err, prediction.ErrInvalidPredictionID)
}
