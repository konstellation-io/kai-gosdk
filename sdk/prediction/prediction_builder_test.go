//go:build integration

package prediction_test

import (
	"time"

	"github.com/konstellation-io/kai-gosdk/sdk/prediction"
)

type predictionBuilder struct {
	predict *prediction.Prediction
}

func newPredictionBuilder() *predictionBuilder {
	return &predictionBuilder{
		predict: &prediction.Prediction{
			CreationDate: time.Now().UnixMilli(),
			LastModified: time.Now().UnixMilli(),
			Payload: prediction.Payload{
				"key-01": "value-01",
			},
			Metadata: prediction.Metadata{
				Product:      _testProduct,
				Version:      _testVersion,
				Workflow:     _testWorkflow,
				WorkflowType: _testWorkflowType,
				Process:      _testProcess,
				RequestID:    _testRequestID,
			},
		},
	}
}

func (pb *predictionBuilder) WithCreationDate(creationDate int64) *predictionBuilder {
	pb.predict.CreationDate = creationDate
	return pb
}

func (pb *predictionBuilder) WithVersion(version string) *predictionBuilder {
	pb.predict.Metadata.Version = version
	return pb
}

func (pb *predictionBuilder) Build() *prediction.Prediction {
	return pb.predict
}
