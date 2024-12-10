//go:build integration

package modelregistry_test

import "github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"

func (s *SdkModelRegistryTestSuite) TestModelRegistry_DeleteModel_ExpectOK() {
	// GIVEN
	modelName := "model.pt"

	modelData1 := []byte("some-data")
	modelVersion1 := "v1.0.0"
	modelDescription1 := "description"
	modelFormat1 := "Pytorch"
	err := s.registerNewModelVersion(
		modelData1,
		modelName,
		modelVersion1,
		modelDescription1,
		modelFormat1,
	)
	s.Assert().NoError(err)

	modelData2 := []byte("some-data2")
	modelVersion2 := "v2.0.0"
	modelDescription2 := "description2"
	modelFormat2 := "Pytorch"
	err = s.registerNewModelVersion(
		modelData2,
		modelName,
		modelVersion2,
		modelDescription2,
		modelFormat2,
	)
	s.Assert().NoError(err)

	// WHEN
	err = s.modelRegistry.DeleteModel(modelName)

	// THEN
	s.Assert().NoError(err)

	modelVersions, err := s.modelRegistry.ListModelVersions(modelName)
	s.Assert().NoError(err)

	s.Assert().Empty(modelVersions)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_DeleteModel_NoModels_ExpectOK() {
	// GIVEN
	modelName := "model.pt"

	// WHEN
	err := s.modelRegistry.DeleteModel(modelName)

	// THEN
	s.Assert().NoError(err)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_DeleteModel_InvalidName_ExpectOK() {
	// GIVEN
	modelName := ""

	// WHEN
	err := s.modelRegistry.DeleteModel(modelName)

	// THEN
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrEmptyName)
}
