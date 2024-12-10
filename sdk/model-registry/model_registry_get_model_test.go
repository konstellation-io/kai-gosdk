//go:build integration

package modelregistry_test

import "github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"

func (s *SdkModelRegistryTestSuite) TestModelRegistry_GetModel_ExpectOK() {
	// GIVEN
	modelData := []byte("some-data")
	modelName := "model.pt"
	modelVersion := "v1.0.0"
	modelDescription := "description"
	modelFormat := "Pytorch"

	err := s.modelRegistry.RegisterModel(
		modelData,
		modelName,
		modelVersion,
		modelFormat,
		modelDescription,
	)
	s.Require().NoError(err)

	// WHEN
	model, err := s.modelRegistry.GetModel(modelName)

	// THEN
	s.Assert().NoError(err)
	s.Assert().NotNil(model)
	s.Assert().Equal(modelData, model.Model)
	s.Assert().Equal(modelVersion, model.Version)
	s.Assert().Equal(modelFormat, model.Format)
	s.Assert().Equal(modelDescription, model.Description)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_GetModelVersion_ExpectOK() {
	// GIVEN
	modelData1 := []byte("some-data")
	modelData2 := []byte("some-data2")
	modelName := "model.pt"
	modelVersion1 := "1.0.0"
	modelVersion2 := "1.0.1"
	modelDescription := "description"
	modelFormat := "Pytorch"

	err := s.modelRegistry.RegisterModel(
		modelData1,
		modelName,
		modelVersion1,
		modelFormat,
		modelDescription,
	)
	s.Require().NoError(err)

	err = s.modelRegistry.RegisterModel(
		modelData2,
		modelName,
		modelVersion2,
		modelFormat,
		modelDescription,
	)
	s.Require().NoError(err)

	// WHEN
	modelV1, err1 := s.modelRegistry.GetModel(modelName, modelVersion1)
	modelV2, err2 := s.modelRegistry.GetModel(modelName, modelVersion2)

	// THEN
	s.Assert().NoError(err1)
	s.Assert().NoError(err2)
	s.Assert().NotNil(modelV1)
	s.Assert().NotNil(modelV2)

	// Assert V1 model is OK
	s.Assert().Equal(modelData1, modelV1.Model)
	s.Assert().Equal(modelVersion1, modelV1.Version)
	s.Assert().Equal(modelFormat, modelV1.Format)
	s.Assert().Equal(modelDescription, modelV1.Description)

	// Assert V2 model is OK
	s.Assert().Equal(modelData2, modelV2.Model)
	s.Assert().Equal(modelVersion2, modelV2.Version)
	s.Assert().Equal(modelFormat, modelV2.Format)
	s.Assert().Equal(modelDescription, modelV2.Description)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_GetNonExistingModel_ExpectError() {
	// GIVEN
	modelName := "non_existing_model.pt"

	// WHEN
	model, err := s.modelRegistry.GetModel(modelName)

	// THEN
	s.Assert().Error(err)
	s.Assert().Nil(model)
	s.Assert().ErrorIs(err, errors.ErrModelNotFound)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_GetNonExistingModelVersion_ExpectError() {
	// GIVEN
	modelData1 := []byte("some-data")
	modelName := "model.pt"
	modelVersion1 := "1.0.0"
	modelVersion2 := "1.0.1"
	modelDescription := "description"
	modelFormat := "Pytorch"

	err := s.modelRegistry.RegisterModel(
		modelData1,
		modelName,
		modelVersion1,
		modelFormat,
		modelDescription,
	)
	s.Require().NoError(err)

	// WHEN
	modelV1, err := s.modelRegistry.GetModel(modelName, modelVersion2)

	// THEN
	s.Assert().Error(err)
	s.Assert().Nil(modelV1)
	s.Assert().ErrorIs(err, errors.ErrModelNotFound)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_GetModel_GetNonExistingModel_Expect() {
	// GIVEN
	modelName := "some-model.pt"

	// WHEN
	model, err := s.modelRegistry.GetModel(modelName)

	// THEN
	s.Assert().Error(err)
	s.Assert().Nil(model)
	s.Assert().ErrorIs(err, errors.ErrModelNotFound)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_GetModel_GetNonExistingModelVersion_Expect() {
	// GIVEN
	modelName := "some-model.pt"
	version := "3.0.0"

	// WHEN
	model, err := s.modelRegistry.GetModel(modelName, version)

	// THEN
	s.Assert().Error(err)
	s.Assert().Nil(model)
	s.Assert().ErrorIs(err, errors.ErrModelNotFound)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_GetModel_EmptyName_Expect() {
	// GIVEN
	modelName := ""

	// WHEN
	model, err := s.modelRegistry.GetModel(modelName)

	// THEN
	s.Assert().Error(err)
	s.Assert().Nil(model)
	s.Assert().ErrorIs(err, errors.ErrEmptyName)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_GetModel_InvalidVersion_Expect() {
	// GIVEN
	modelName := "model.pt"
	version := "My invalid version v1"

	// WHEN
	model, err := s.modelRegistry.GetModel(modelName, version)

	// THEN
	s.Assert().Error(err)
	s.Assert().Nil(model)
	s.Assert().ErrorIs(err, errors.ErrInvalidVersion)
}
