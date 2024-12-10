//go:build integration

package modelregistry_test

func (s *SdkModelRegistryTestSuite) TestModelRegistry_ListModels_ExpectOK() {
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
	models, err := s.modelRegistry.ListModels()

	// THEN
	s.Assert().NoError(err)
	s.Assert().NotNil(models)
	s.Assert().Len(models, 1)
	s.Assert().Equal(modelName, models[0].Name)
	s.Assert().Equal(modelFormat, models[0].Format)
	s.Assert().Equal(modelDescription, models[0].Description)
	s.Assert().Equal(modelVersion, models[0].Version)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_ListModels_NoModels_ExpectOK() {
	// WHEN
	models, err := s.modelRegistry.ListModels()

	// THEN
	s.Assert().NoError(err)
	s.Assert().Empty(models)
}
