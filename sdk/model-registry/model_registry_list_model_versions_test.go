//go:build integration

package modelregistry_test

import (
	modelregistry "github.com/konstellation-io/kai-gosdk/sdk/model-registry"
)

func (s *SdkModelRegistryTestSuite) TestModelRegistry_ListModelVersions_ExpectOK() {
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
	modelVersions, err := s.modelRegistry.ListModelVersions(modelName)

	// THEN
	s.Assert().NoError(err)
	s.Assert().NotNil(modelVersions)
	s.Assert().Len(modelVersions, 2)
	s.Assert().Equal(modelVersions[1], &modelregistry.ModelInfo{
		Name:        modelName,
		Version:     modelVersion1,
		Description: modelDescription1,
		Format:      modelFormat1,
	})
	s.Assert().Equal(modelVersions[0], &modelregistry.ModelInfo{
		Name:        modelName,
		Version:     modelVersion2,
		Description: modelDescription2,
		Format:      modelFormat2,
	})
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_ListModelVersions_NoModels_ExpectOK() {
	// GIVEN
	modelName := "model.pt"

	// WHEN
	models, err := s.modelRegistry.ListModelVersions(modelName)

	// THEN
	s.Assert().NoError(err)
	s.Assert().Empty(models)
}
