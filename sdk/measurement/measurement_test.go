//go:build unit

package measurement_test

import (
	"testing"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/measurement"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/metadata"
)

type SdkMeasurementTestSuite struct {
	suite.Suite
	logger logr.Logger
}

func (s *SdkMeasurementTestSuite) SetupSuite() {
	s.logger = testr.NewWithOptions(s.T(), testr.Options{Verbosity: 1, LogTimestamp: true})
}

func (s *SdkMeasurementTestSuite) SetupTest() {
	// Reset viper values before each test
	viper.Reset()
}

func (s *SdkMeasurementTestSuite) TestMetadata_GetMetadata_ExpectOK() {
	// Given
	viper.SetDefault(common.ConfigMetadataProductIDKey, "product-name")
	viper.SetDefault(common.ConfigMetadataWorkflowIDKey, "workflow-name")
	viper.SetDefault(common.ConfigMetadataProcessIDKey, "process-name")
	viper.SetDefault(common.ConfigMetadataVersionIDKey, "version-name")
	viper.SetDefault(common.ConfigNatsEphemeralStorage, "my-ephemeral-storage")
	viper.SetDefault(common.ConfigCcGlobalBucketKey, "global-bucket")
	viper.SetDefault(common.ConfigCcProductBucketKey, "product-bucket")
	viper.SetDefault(common.ConfigCcWorkflowBucketKey, "workflow-bucket")
	viper.SetDefault(common.ConfigCcProcessBucketKey, "process-bucket")
	viper.SetDefault(common.ConfigMeasurementsEndpointKey, "http://localhost:4137")
	viper.SetDefault(common.ConfigMeasurementsInsecureKey, true)
	viper.SetDefault(common.ConfigMeasurementsTimeoutKey, 10)
	viper.SetDefault(common.ConfigMeasurementsMetricsIntervalKey, 10)

	// When
	sdkMetadata := metadata.New()
	sdkMeasurement, err := measurement.New(s.logger, sdkMetadata)

	// Then
	s.NotNil(sdkMetadata)
	s.NotNil(sdkMeasurement)
	s.Nil(err)
}

func TestSdkMetadataTestSuite(t *testing.T) {
	suite.Run(t, new(SdkMeasurementTestSuite))
}
