//go:build unit

package prediction_test

import (
	"testing"
	"time"

	"github.com/konstellation-io/kai-gosdk/sdk/prediction"
	"github.com/stretchr/testify/assert"
)

func TestFilter_Validate(t *testing.T) {
	testCases := []struct {
		name          string
		filter        prediction.Filter
		expectedError error
	}{
		{
			name: "valid filter",
			filter: prediction.Filter{
				CreationDate: prediction.TimestampRange{
					StartDate: time.Now().Add(-10 * time.Minute),
					EndDate:   time.Now().Add(10 * time.Minute),
				},
			},
			expectedError: nil,
		},
		{
			name: "filter without start date is not valid",
			filter: prediction.Filter{
				CreationDate: prediction.TimestampRange{
					EndDate: time.Now(),
				},
			},
			expectedError: prediction.ErrDateRangeFilterRequired,
		},
		{
			name: "filter without creation date range is not valid",
			filter: prediction.Filter{
				CreationDate: prediction.TimestampRange{},
			},
			expectedError: prediction.ErrDateRangeFilterRequired,
		},
		{
			name: "filter without end date is not valid",
			filter: prediction.Filter{
				CreationDate: prediction.TimestampRange{
					StartDate: time.Now(),
				},
			},
			expectedError: prediction.ErrDateRangeFilterRequired,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorIs(t, tc.filter.Validate(), tc.expectedError)
		})
	}
}
