//go:build unit

package centralizedconfiguration_test

import (
	centralizedConfiguration "github.com/konstellation-io/kai-gosdk/sdk/centralized-configuration"
	"github.com/nats-io/nats.go"
)

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_DeleteConfigOnWorkflowScope_ExpectOK() {
	// Given
	s.workflowKv.On("Delete", "key1").Return(nil)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.DeleteConfig("key1", centralizedConfiguration.WorkflowScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.workflowKv.AssertNumberOfCalls(s.T(), "Delete", 1)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_DeleteConfigOnProcessScope_ExpectOK() {
	// Given
	s.processKv.On("Delete", "key1").Return(nil)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.DeleteConfig("key1", centralizedConfiguration.ProcessScope)
	s.Require().NoError(err)

	// Then
	s.NotNil(config)
	s.processKv.AssertNumberOfCalls(s.T(), "Delete", 1)
}

func (s *SdkCentralizedConfigurationTestSuite) TestCentralizedConfiguration_DeleteNonExistingConfigOnAllScope_ExpectError() {
	// Given
	s.globalKv.On("Delete", "key1").Return(nats.ErrKeyNotFound)
	s.productKv.On("Delete", "key1").Return(nats.ErrKeyNotFound)
	s.workflowKv.On("Delete", "key1").Return(nats.ErrKeyNotFound)
	s.processKv.On("Delete", "key1").Return(nats.ErrKeyNotFound)

	config, err := centralizedConfiguration.NewBuilder(
		s.logger,
		&s.globalKv,
		&s.productKv,
		&s.workflowKv,
		&s.processKv,
	)
	s.Require().NoError(err)

	// When
	err = config.DeleteConfig("key1", centralizedConfiguration.GlobalScope)
	s.Error(err)
	err = config.DeleteConfig("key1", centralizedConfiguration.ProductScope)
	s.Error(err)
	err = config.DeleteConfig("key1", centralizedConfiguration.WorkflowScope)
	s.Error(err)
	err = config.DeleteConfig("key1", centralizedConfiguration.ProcessScope)
	s.Error(err)

	// Then
	s.NotNil(config)
	s.globalKv.AssertNumberOfCalls(s.T(), "Delete", 1)
	s.productKv.AssertNumberOfCalls(s.T(), "Delete", 1)
	s.workflowKv.AssertNumberOfCalls(s.T(), "Delete", 1)
	s.processKv.AssertNumberOfCalls(s.T(), "Delete", 1)
}
