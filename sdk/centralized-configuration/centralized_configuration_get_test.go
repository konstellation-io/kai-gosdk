//go:build unit

package centralizedconfiguration_test

import (
	"errors"
	"fmt"

	"github.com/konstellation-io/kai-gosdk/mocks"
	centralizedConfiguration "github.com/konstellation-io/kai-gosdk/sdk/centralized-configuration"
	"github.com/nats-io/nats.go"
)

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetConfigOnAllScopes_ExpectOK() {
	// Given
	globalResponse := mocks.NewKeyValueEntryMock(s.T())

	globalResponse.On("Value").Return([]byte("value0"))

	productResponse := mocks.NewKeyValueEntryMock(s.T())

	productResponse.On("Value").Return([]byte("value1"))

	workflowResponse := mocks.NewKeyValueEntryMock(s.T())

	workflowResponse.On("Value").Return([]byte("value2"))

	processResponse := mocks.NewKeyValueEntryMock(s.T())

	processResponse.On("Value").Return([]byte("value3"))

	s.globalKv.On("Get", "key0").Return(globalResponse, nil)
	s.productKv.On("Get", "key0").Return(nil, nats.ErrKeyNotFound)
	s.workflowKv.On("Get", "key0").Return(nil, nats.ErrKeyNotFound)
	s.processKv.On("Get", "key0").Return(nil, nats.ErrKeyNotFound)

	s.globalKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)
	s.productKv.On("Get", "key1").Return(productResponse, nil)
	s.workflowKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)
	s.processKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)

	s.globalKv.On("Get", "key2").Return(nil, nats.ErrKeyNotFound)
	s.productKv.On("Get", "key2").Return(nil, nats.ErrKeyNotFound)
	s.workflowKv.On("Get", "key2").Return(workflowResponse, nil)
	s.processKv.On("Get", "key2").Return(nil, nats.ErrKeyNotFound)

	s.globalKv.On("Get", "key3").Return(nil, nats.ErrKeyNotFound)
	s.productKv.On("Get", "key3").Return(nil, nats.ErrKeyNotFound)
	s.workflowKv.On("Get", "key3").Return(nil, nats.ErrKeyNotFound)
	s.processKv.On("Get", "key3").Return(processResponse, nil)
	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key0Value, err := config.GetConfig("key0")
	s.Require().NoError(err)
	key1Value, err := config.GetConfig("key1")
	s.Require().NoError(err)
	key2Value, err := config.GetConfig("key2")
	s.Require().NoError(err)
	key3Value, err := config.GetConfig("key3")
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.globalKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.Equal("value0", key0Value)
	s.productKv.AssertNumberOfCalls(s.T(), "Get", 2)
	s.Equal("value1", key1Value)
	s.workflowKv.AssertNumberOfCalls(s.T(), "Get", 3)
	s.Equal("value2", key2Value)
	s.processKv.AssertNumberOfCalls(s.T(), "Get", 4)
	s.Equal("value3", key3Value)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetNonExistingConfigOnAllScopes_ExpectError() {
	// Given
	s.globalKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)
	s.productKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)
	s.workflowKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)
	s.processKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1")

	// Then
	s.Require().Error(err)
	s.Assert().ErrorIs(err, centralizedConfiguration.ErrKeyNotFound)
	s.NotNil(config)
	s.Empty(key1Value)
	s.globalKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.productKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.workflowKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.processKv.AssertNumberOfCalls(s.T(), "Get", 1)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetNonExistingConfigOnAllScopes_UnexpectedError_ExpectError() {
	// Given
	s.globalKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)
	s.productKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)
	s.workflowKv.On("Get", "key1").Return(nil, errors.New("unexpected error"))
	s.processKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1")

	// Then
	s.Require().Error(err)
	s.Assert().NotErrorIs(err, centralizedConfiguration.ErrKeyNotFound)
	s.NotNil(config)
	s.Empty(key1Value)
	s.globalKv.AssertNumberOfCalls(s.T(), "Get", 0)
	s.productKv.AssertNumberOfCalls(s.T(), "Get", 0)
	s.workflowKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.processKv.AssertNumberOfCalls(s.T(), "Get", 1)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetOverridenConfigOnAllScopes_ExpectOK() {
	// Given
	globalResponse := mocks.NewKeyValueEntryMock(s.T())
	productResponse := mocks.NewKeyValueEntryMock(s.T())
	workflowResponse := mocks.NewKeyValueEntryMock(s.T())
	workflowResponse.On("Value").Return([]byte("workflowValue"))

	processResponse := mocks.NewKeyValueEntryMock(s.T())
	processResponse.On("Value").Return([]byte("processValue"))

	s.globalKv.On("Get", "key1").Return(globalResponse, nil)
	s.productKv.On("Get", "key1").Return(productResponse, nil)
	s.workflowKv.On("Get", "key1").Return(workflowResponse, nil)
	s.processKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)

	s.globalKv.On("Get", "key2").Return(globalResponse, nil)
	s.productKv.On("Get", "key2").Return(productResponse, nil)
	s.workflowKv.On("Get", "key2").Return(workflowResponse, nil)
	s.processKv.On("Get", "key2").Return(processResponse, nil)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1")
	s.Require().NoError(err)
	key2Value, err := config.GetConfig("key2")
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.globalKv.AssertNotCalled(s.T(), "Get")
	s.productKv.AssertNotCalled(s.T(), "Get")
	s.workflowKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.processKv.AssertNumberOfCalls(s.T(), "Get", 2)
	s.Equal("workflowValue", key1Value)
	s.Equal("processValue", key2Value)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetConfigOnGlobalScope_ExpectOK() {
	// Given
	globalResponse := mocks.NewKeyValueEntryMock(s.T())
	globalResponse.On("Value").Return([]byte("value1"))

	s.globalKv.On("Get", "key1").Return(globalResponse, nil)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1", centralizedConfiguration.GlobalScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.globalKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.Equal("value1", key1Value)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetNonExistingConfigOnGlobalScope_ExpectError() {
	// Given
	s.globalKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1", centralizedConfiguration.GlobalScope)

	// Then
	s.Error(err)
	s.NotNil(config)
	s.Empty(key1Value)
	s.globalKv.AssertNumberOfCalls(s.T(), "Get", 1)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetConfigOnProductScope_ExpectOK() {
	// Given
	productResponse := mocks.NewKeyValueEntryMock(s.T())
	productResponse.On("Value").Return([]byte("value1"))

	s.productKv.On("Get", "key1").Return(productResponse, nil)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1", centralizedConfiguration.ProductScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.productKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.Equal("value1", key1Value)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetNonExistingConfigOnProductScope_ExpectError() {
	// Given
	s.productKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1", centralizedConfiguration.ProductScope)

	// Then
	s.Error(err)
	s.NotNil(config)
	s.Empty(key1Value)
	s.productKv.AssertNumberOfCalls(s.T(), "Get", 1)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetConfigOnWorkflowScope_ExpectOK() {
	// Given
	workflowResponse := mocks.NewKeyValueEntryMock(s.T())
	workflowResponse.On("Value").Return([]byte("value2"))

	s.workflowKv.On("Get", "key1").Return(workflowResponse, nil)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1", centralizedConfiguration.WorkflowScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.workflowKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.Equal("value2", key1Value)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetNonExistingConfigOnWorkflowScope_ExpectError() {
	// Given
	s.workflowKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1", centralizedConfiguration.WorkflowScope)

	// Then
	s.Error(err)
	s.Assert().ErrorIs(err, centralizedConfiguration.ErrKeyNotFound)
	s.NotNil(config)
	s.Empty(key1Value)
	s.workflowKv.AssertNumberOfCalls(s.T(), "Get", 1)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetConfigOnProcessScope_ExpectOK() {
	// Given
	processResponse := mocks.NewKeyValueEntryMock(s.T())
	processResponse.On("Value").Return([]byte("value3"))

	s.processKv.On("Get", "key1").Return(processResponse, nil)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1", centralizedConfiguration.ProcessScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.processKv.AssertNumberOfCalls(s.T(), "Get", 1)
	s.Equal("value3", key1Value)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetNonExistingConfigOnProcessScope_ExpectError() {
	// Given
	s.processKv.On("Get", "key1").Return(nil, nats.ErrKeyNotFound)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1", centralizedConfiguration.ProcessScope)

	// Then
	s.Error(err)
	s.NotNil(config)
	s.Empty(key1Value)
	s.processKv.AssertNumberOfCalls(s.T(), "Get", 1)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_GetConfigOnProcessScope_ExpectError() {
	// Given
	s.processKv.On("Get", "key1").Return(nil, fmt.Errorf("unexpected error"))

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	key1Value, err := config.GetConfig("key1", centralizedConfiguration.ProcessScope)

	// Then
	s.Error(err)
	s.NotNil(config)
	s.Empty(key1Value)
	s.processKv.AssertNumberOfCalls(s.T(), "Get", 1)
}
