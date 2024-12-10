//go:build unit

package centralizedconfiguration_test

import (
	"errors"
	"testing"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"

	centralizedConfiguration "github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/centralized-configuration"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/mocks"
)

const (
	globalBucketProp       = common.ConfigCcGlobalBucketKey
	globalBucketVal        = "global-bucket"
	wrongGlobalBucketVal   = "some-global-bucket"
	productBucketProp      = common.ConfigCcProductBucketKey
	productBucketVal       = "product-bucket"
	wrongProductBucketVal  = "some-product-bucket"
	workflowBucketProp     = common.ConfigCcWorkflowBucketKey
	workflowBucketVal      = "workflow-bucket"
	wrongWorkflowBucketVal = "some-workflow-bucket"
	processBucketProp      = common.ConfigCcProcessBucketKey
	processBucketVal       = "process-bucket"
	wrongProcessBucketVal  = "some-process-bucket"
	keyValue               = "KeyValue"
	notExistMessage        = "not exist"
)

//go:generate mockery --dir $GOPATH/pkg/mod/github.com/nats-io/nats.go@v1.31.0 --output ../../mocks --name KeyValue --structname KeyValueMock --filename key_value_mock.go
//go:generate mockery --dir $GOPATH/pkg/mod/github.com/nats-io/nats.go@v1.31.0 --output ../../mocks --name KeyValueEntry --structname KeyValueEntryMock --filename key_value_entry_mock.go
//go:generate mockery --dir $GOPATH/pkg/mod/github.com/nats-io/nats.go@v1.31.0 --output ../../mocks --name JetStreamContext --structname JetStreamContextMock --filename jetstream_context_mock.go
type SdkCentralizedConfigurationTestSuite struct {
	suite.Suite
	logger     logr.Logger
	jetstream  mocks.JetStreamContextMock
	globalKv   mocks.KeyValueMock
	productKv  mocks.KeyValueMock
	workflowKv mocks.KeyValueMock
	processKv  mocks.KeyValueMock
}

func (s *SdkCentralizedConfigurationTestSuite) SetupSuite() {
	s.logger = testr.NewWithOptions(s.T(), testr.Options{Verbosity: 1})
}

func (s *SdkCentralizedConfigurationTestSuite) SetupTest() {
	// Reset viper values before each test
	viper.Reset()

	s.jetstream = *mocks.NewJetStreamContextMock(s.T())
	s.globalKv = *mocks.NewKeyValueMock(s.T())
	s.productKv = *mocks.NewKeyValueMock(s.T())
	s.workflowKv = *mocks.NewKeyValueMock(s.T())
	s.processKv = *mocks.NewKeyValueMock(s.T())
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfig_InitializeConfigurationScopes_ExpectOK() {
	// Given
	viper.SetDefault(globalBucketProp, globalBucketVal)
	viper.SetDefault(productBucketProp, productBucketVal)
	viper.SetDefault(workflowBucketProp, workflowBucketVal)
	viper.SetDefault(processBucketProp, processBucketVal)

	s.jetstream.On(keyValue, globalBucketVal).Return(&s.globalKv, nil)
	s.jetstream.On(keyValue, productBucketVal).Return(&s.productKv, nil)
	s.jetstream.On(keyValue, workflowBucketVal).Return(&s.workflowKv, nil)
	s.jetstream.On(keyValue, processBucketVal).Return(&s.processKv, nil)

	// When
	conf, err := centralizedConfiguration.New(s.logger, &s.jetstream)

	// Then
	s.NoError(err)
	s.NotNil(conf)
}

func (s *SdkCentralizedConfigurationTestSuite) TestInitializeConfigScopes_GlobalConfigNotExist_ExpectError() {
	// Given
	viper.SetDefault(globalBucketProp, wrongGlobalBucketVal)
	viper.SetDefault(productBucketProp, wrongProductBucketVal)
	viper.SetDefault(workflowBucketProp, wrongWorkflowBucketVal)
	viper.SetDefault(processBucketProp, wrongProcessBucketVal)

	s.jetstream.On(keyValue, wrongGlobalBucketVal).Return(nil, errors.New(notExistMessage))
	s.jetstream.On(keyValue, wrongProductBucketVal).Return(&s.productKv, nil)
	s.jetstream.On(keyValue, wrongWorkflowBucketVal).Return(&s.workflowKv, nil)
	s.jetstream.On(keyValue, wrongProcessBucketVal).Return(&s.processKv, nil)
	// When
	config, err := centralizedConfiguration.New(s.logger, &s.jetstream)

	// Then
	s.Error(err)
	s.Nil(config)
}

func (s *SdkCentralizedConfigurationTestSuite) TestInitializeConfigScopes_ProductConfigNotExist_ExpectError() {
	// Given
	viper.SetDefault(globalBucketProp, wrongGlobalBucketVal)
	viper.SetDefault(productBucketProp, wrongProductBucketVal)
	viper.SetDefault(workflowBucketProp, wrongWorkflowBucketVal)
	viper.SetDefault(processBucketProp, wrongProcessBucketVal)

	s.jetstream.On(keyValue, wrongGlobalBucketVal).Return(&s.globalKv, nil)
	s.jetstream.On(keyValue, wrongProductBucketVal).Return(nil, errors.New(notExistMessage))
	s.jetstream.On(keyValue, wrongWorkflowBucketVal).Return(&s.workflowKv, nil)
	s.jetstream.On(keyValue, wrongProcessBucketVal).Return(&s.processKv, nil)
	// When
	config, err := centralizedConfiguration.New(s.logger, &s.jetstream)

	// Then
	s.Error(err)
	s.Nil(config)
}

func (s *SdkCentralizedConfigurationTestSuite) TestInitializeConfigScopes_WorkflowConfigNotExist_ExpectError() {
	// Given
	viper.SetDefault(globalBucketProp, wrongGlobalBucketVal)
	viper.SetDefault(productBucketProp, wrongProductBucketVal)
	viper.SetDefault(workflowBucketProp, wrongWorkflowBucketVal)
	viper.SetDefault(processBucketProp, wrongProcessBucketVal)

	s.jetstream.On(keyValue, wrongGlobalBucketVal).Return(&s.globalKv, nil)
	s.jetstream.On(keyValue, wrongProductBucketVal).Return(&s.productKv, nil)
	s.jetstream.On(keyValue, wrongWorkflowBucketVal).Return(nil, errors.New(notExistMessage))
	s.jetstream.On(keyValue, wrongProcessBucketVal).Return(&s.processKv, nil)

	// When
	config, err := centralizedConfiguration.New(s.logger, &s.jetstream)

	// Then
	s.Error(err)
	s.Nil(config)
}

func (s *SdkCentralizedConfigurationTestSuite) TestInitializeConfigScopes_ProcessConfigNotExist_ExpectError() {
	// Given
	viper.SetDefault(globalBucketProp, wrongGlobalBucketVal)
	viper.SetDefault(productBucketProp, wrongProductBucketVal)
	viper.SetDefault(workflowBucketProp, wrongWorkflowBucketVal)
	viper.SetDefault(processBucketProp, wrongProcessBucketVal)

	s.jetstream.On(keyValue, wrongGlobalBucketVal).Return(&s.globalKv, nil)
	s.jetstream.On(keyValue, wrongProductBucketVal).Return(&s.productKv, nil)
	s.jetstream.On(keyValue, wrongWorkflowBucketVal).Return(&s.workflowKv, nil)
	s.jetstream.On(keyValue, wrongProcessBucketVal).Return(nil, errors.New(notExistMessage))

	// When
	config, err := centralizedConfiguration.New(s.logger, &s.jetstream)

	// Then
	s.Error(err)
	s.Nil(config)
}

func (s *SdkCentralizedConfigurationTestSuite) TestDeleteConfigOnProductScope_ExpectOK() {
	// Given
	s.productKv.On("Delete", "key1").Return(nil)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.DeleteConfig("key1", centralizedConfiguration.ProductScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.productKv.AssertNumberOfCalls(s.T(), "Delete", 1)
}

func TestSdkCentralizedConfigTestSuite(t *testing.T) {
	suite.Run(t, new(SdkCentralizedConfigurationTestSuite))
}
