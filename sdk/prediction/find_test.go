//go:build integration

package prediction_test

import (
	"context"
	"time"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/prediction"
)

func (s *PredictionStoreSuite) TestPredictionStore_Find_ExpectOK() {
	var (
		ctx             = context.Background()
		firstPrediction = prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: s.getPredictionMetadata(),
		}

		secondPrediction = prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: s.getPredictionMetadata(),
		}

		expectedPredictions = []prediction.Prediction{firstPrediction, secondPrediction}
	)

	err := s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("first-prediction"), "$", firstPrediction).
		Err()
	s.Require().NoError(err)

	err = s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("second-prediction"), "$", secondPrediction).
		Err()
	s.Require().NoError(err)

	actualPredictions, err := s.predictionStore.Find(ctx, &prediction.Filter{
		Version: _testVersion,
		CreationDate: prediction.TimestampRange{
			StartDate: time.Now().Add(-10 * time.Minute),
			EndDate:   time.Now(),
		},
	})
	s.Require().NoError(err)

	// THEN
	s.ElementsMatch(expectedPredictions, actualPredictions)
}

func (s *PredictionStoreSuite) TestPredictionStore_Find_FilterByMultipleFields_ExpectOK() {
	var (
		ctx             = context.Background()
		firstPrediction = prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: s.getPredictionMetadata(),
		}

		secondPrediction = prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: prediction.Metadata{
				Product:   firstPrediction.Metadata.Product,
				Version:   firstPrediction.Metadata.Version,
				Workflow:  "different-workflow",
				Process:   firstPrediction.Metadata.Process,
				RequestID: firstPrediction.Metadata.RequestID,
			},
		}

		expectedPredictions = []prediction.Prediction{firstPrediction}
	)

	err := s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("first-prediction"), "$", firstPrediction).
		Err()
	s.Require().NoError(err)

	err = s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("second-prediction"), "$", secondPrediction).
		Err()
	s.Require().NoError(err)

	actualPredictions, err := s.predictionStore.Find(ctx, &prediction.Filter{
		Version:   _testVersion,
		Workflow:  _testWorkflow,
		Process:   _testProcess,
		RequestID: _testRequestID,
		CreationDate: prediction.TimestampRange{
			StartDate: time.Now().Add(-10 * time.Minute),
			EndDate:   time.Now(),
		},
	})
	s.Require().NoError(err)

	// THEN
	s.ElementsMatch(expectedPredictions, actualPredictions)
}

func (s *PredictionStoreSuite) TestPredictionStore_Find_FilterByTimestampRange_ExpectOK() {
	var (
		ctx             = context.Background()
		firstPrediction = prediction.Prediction{
			CreationDate: 1000,
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: s.getPredictionMetadata(),
		}

		secondPrediction = prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: s.getPredictionMetadata(),
		}

		expectedPredictions = []prediction.Prediction{secondPrediction}
	)

	err := s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("first-prediction"), "$", firstPrediction).
		Err()
	s.Require().NoError(err)

	err = s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("second-prediction"), "$", secondPrediction).
		Err()
	s.Require().NoError(err)

	actualPredictions, err := s.predictionStore.Find(ctx, &prediction.Filter{
		CreationDate: prediction.TimestampRange{
			StartDate: time.Now().Add(-10 * time.Minute),
			EndDate:   time.Now().Add(10 * time.Minute),
		},
	})
	s.Require().NoError(err)

	// THEN
	s.ElementsMatch(expectedPredictions, actualPredictions)
}

func (s *PredictionStoreSuite) TestPredictionStore_Find_FailsIfIndexDoesNotExist() {
	var (
		ctx = context.Background()
	)

	// FlushAll deletes index too
	err := s.redisClient.FlushAll(ctx).Err()
	s.Require().NoError(err)

	actualPredictions, err := s.predictionStore.Find(ctx, &prediction.Filter{
		Version: _testVersion,
	})

	// THEN
	s.Error(err)
	s.Empty(actualPredictions)
}

func (s *PredictionStoreSuite) TestPredictionStore_Find_ProductFilterAppliedByDefault_ExpectOK() {
	var (
		ctx             = context.Background()
		firstPrediction = prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: s.getPredictionMetadata(),
		}

		secondPrediction = prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: prediction.Metadata{
				Product:   "different-product",
				Version:   firstPrediction.Metadata.Version,
				Workflow:  firstPrediction.Metadata.Version,
				Process:   firstPrediction.Metadata.Process,
				RequestID: firstPrediction.Metadata.RequestID,
			},
		}

		expectedPredictions = []prediction.Prediction{firstPrediction}
	)

	err := s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("first-prediction"), "$", firstPrediction).
		Err()
	s.Require().NoError(err)

	err = s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("second-prediction"), "$", secondPrediction).
		Err()
	s.Require().NoError(err)

	actualPredictions, err := s.predictionStore.Find(ctx, &prediction.Filter{
		Version: _testVersion,
		CreationDate: prediction.TimestampRange{
			StartDate: time.Now().Add(-10 * time.Minute),
			EndDate:   time.Now(),
		},
	})
	s.Require().NoError(err)

	// THEN
	s.ElementsMatch(expectedPredictions, actualPredictions)
}

func (s *PredictionStoreSuite) TestPredictionStore_Find_FilterByActualVersionByDefault() {
	var (
		ctx             = context.Background()
		firstPrediction = prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: s.getPredictionMetadata(),
		}

		secondPrediction = prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"test-key-2": "test-value-2",
			},
			Metadata: prediction.Metadata{
				Product:      _testProduct,
				Version:      "another-version",
				Workflow:     _testWorkflow,
				WorkflowType: _testWorkflowType,
				Process:      _testProcess,
				RequestID:    _testRequestID,
			},
		}

		expectedPredictions = []prediction.Prediction{firstPrediction}
	)

	err := s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("first-prediction"), "$", firstPrediction).
		Err()
	s.Require().NoError(err)

	err = s.redisClient.
		JSONSet(ctx, s.getKeyWithProductPrefix("second-prediction"), "$", secondPrediction).
		Err()
	s.Require().NoError(err)

	actualPredictions, err := s.predictionStore.Find(ctx, &prediction.Filter{
		CreationDate: prediction.TimestampRange{
			StartDate: time.Now().Add(-10 * time.Minute),
			EndDate:   time.Now(),
		},
	})
	s.Require().NoError(err)

	// THEN
	s.ElementsMatch(expectedPredictions, actualPredictions)
}
