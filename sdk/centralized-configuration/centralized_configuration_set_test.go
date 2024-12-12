//go:build unit

package centralizedconfiguration_test

import (
	"errors"

	centralizedConfiguration "github.com/konstellation-io/kai-gosdk/sdk/centralized-configuration"
)

const notExist = "not exist"

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_SetConfigOnDefaultScope_ExpectOK() {
	// Given
	s.globalKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.productKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.workflowKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.processKv.On("PutString", "key1", "value1").Return(uint64(1), nil)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.SetConfig("key1", "value1")
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.globalKv.AssertNotCalled(s.T(), "PutString")
	s.productKv.AssertNotCalled(s.T(), "PutString")
	s.workflowKv.AssertNotCalled(s.T(), "PutString")
	s.processKv.AssertNumberOfCalls(s.T(), "PutString", 1)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_SetConfigOnProductScope_ExpectOK() {
	// Given
	s.globalKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.productKv.On("PutString", "key1", "value1").Return(uint64(1), nil)
	s.workflowKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.processKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.SetConfig("key1", "value1", centralizedConfiguration.ProductScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.globalKv.AssertNotCalled(s.T(), "PutString")
	s.productKv.AssertNumberOfCalls(s.T(), "PutString", 1)
	s.productKv.AssertCalled(s.T(), "PutString", "key1", "value1")
	s.workflowKv.AssertNotCalled(s.T(), "PutString")
	s.processKv.AssertNotCalled(s.T(), "PutString")
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_SetConfigOnWorkflowScope_ExpectOK() {
	// Given
	s.globalKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.productKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.workflowKv.On("PutString", "key1", "value1").Return(uint64(1), nil)
	s.processKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.SetConfig("key1", "value1", centralizedConfiguration.WorkflowScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.globalKv.AssertNotCalled(s.T(), "PutString")
	s.productKv.AssertNotCalled(s.T(), "PutString")
	s.workflowKv.AssertNumberOfCalls(s.T(), "PutString", 1)
	s.workflowKv.AssertCalled(s.T(), "PutString", "key1", "value1")
	s.processKv.AssertNotCalled(s.T(), "PutString")
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_SetConfigOnProcessScope_ExpectOK() {
	// Given
	s.globalKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.productKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.workflowKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.processKv.On("PutString", "key1", "value1").Return(uint64(1), nil)
	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.SetConfig("key1", "value1", centralizedConfiguration.ProcessScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.globalKv.AssertNotCalled(s.T(), "PutString")
	s.productKv.AssertNotCalled(s.T(), "PutString")
	s.workflowKv.AssertNotCalled(s.T(), "PutString")
	s.processKv.AssertNumberOfCalls(s.T(), "PutString", 1)
	s.processKv.AssertCalled(s.T(), "PutString", "key1", "value1")
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_SetConfigOnGlobalScope_ExpectOK() {
	// Given
	s.globalKv.On("PutString", "key1", "value1").Return(uint64(1), nil)
	s.productKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.workflowKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.processKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.SetConfig("key1", "value1", centralizedConfiguration.GlobalScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.globalKv.AssertNumberOfCalls(s.T(), "PutString", 1)
	s.globalKv.AssertCalled(s.T(), "PutString", "key1", "value1")
	s.productKv.AssertNotCalled(s.T(), "PutString")
	s.workflowKv.AssertNotCalled(s.T(), "PutString")
	s.processKv.AssertNotCalled(s.T(), "PutString")
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_SetConfigOnDefaultScope_UnexpectedError_ExpectError() {
	// Given
	s.globalKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.productKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.workflowKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))
	s.processKv.On("PutString", "key1", "value1").Return(uint64(0), errors.New(notExist))

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.SetConfig("key1", "value1")

	// Then
	s.Error(err)
	s.NotNil(config)
	s.globalKv.AssertNotCalled(s.T(), "PutString")
	s.productKv.AssertNotCalled(s.T(), "PutString")
	s.workflowKv.AssertNotCalled(s.T(), "PutString")
	s.processKv.AssertNumberOfCalls(s.T(), "PutString", 1)
}
