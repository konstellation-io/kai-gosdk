//go:build unit

package metadata_test

import (
	"testing"

	"github.com/konstellation-io/kai-gosdk/internal/common"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"

	"github.com/konstellation-io/kai-gosdk/sdk/metadata"
)

type SdkMetadataTestSuite struct {
	suite.Suite
	logger logr.Logger
}

func (s *SdkMetadataTestSuite) SetupSuite() {
	s.logger = testr.NewWithOptions(s.T(), testr.Options{Verbosity: 1, LogTimestamp: true})
}

func (s *SdkMetadataTestSuite) SetupTest() {
	// Reset viper values before each test
	viper.Reset()
}

func (s *SdkMetadataTestSuite) TestMetadata_GetMetadata_ExpectOK() {
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

	// When
	sdkMetadata := metadata.New()

	productName := sdkMetadata.GetProduct()
	workflowName := sdkMetadata.GetWorkflow()
	processName := sdkMetadata.GetProcess()
	versionName := sdkMetadata.GetVersion()
	objectStoreName := sdkMetadata.GetEphemeralStorageName()
	globalKvName := sdkMetadata.GetGlobalCentralizedConfigurationName()
	productKvName := sdkMetadata.GetProductCentralizedConfigurationName()
	workflowKvName := sdkMetadata.GetWorkflowCentralizedConfigurationName()
	processKvName := sdkMetadata.GetProcessCentralizedConfigurationName()

	// Then
	s.NotNil(sdkMetadata)
	s.Equal("product-name", productName)
	s.Equal("workflow-name", workflowName)
	s.Equal("process-name", processName)
	s.Equal("version-name", versionName)
	s.Equal("my-ephemeral-storage", objectStoreName)
	s.Equal("global-bucket", globalKvName)
	s.Equal("product-bucket", productKvName)
	s.Equal("workflow-bucket", workflowKvName)
	s.Equal("process-bucket", processKvName)
}

func TestSdkMetadataTestSuite(t *testing.T) {
	suite.Run(t, new(SdkMetadataTestSuite))
}
