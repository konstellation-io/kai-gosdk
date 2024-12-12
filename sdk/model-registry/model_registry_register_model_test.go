//go:build integration

package modelregistry_test

import (
	"context"
	"io"
	"path"

	"github.com/konstellation-io/kai-gosdk/internal/common"
	"github.com/konstellation-io/kai-gosdk/internal/errors"
	"github.com/minio/minio-go/v7"
	"github.com/spf13/viper"
)

func (s *SdkModelRegistryTestSuite) TestModelRegistry_RegisterModel_ExpectOK() {
	// GIVEN
	modelData := []byte("some-data")
	modelName := "model.pt"
	modelVersion := "v1.0.0"
	modelDescription := "description"
	modelFormat := "Pytorch"

	// WHEN
	err := s.modelRegistry.RegisterModel(
		modelData,
		modelName,
		modelVersion,
		modelFormat,
		modelDescription,
	)
	s.Assert().NoError(err)

	// THEN
	obj, err := s.client.GetObject(
		context.Background(),
		s.modelRegistryBucket,
		path.Join(
			viper.GetString(common.ConfigMinioInternalFolderKey),
			viper.GetString(common.ConfigModelFolderNameKey),
			modelName,
		),
		minio.GetObjectOptions{},
	)
	s.Require().NoError(err)

	objData, err := io.ReadAll(obj)
	s.Require().NoError(err)

	stats, err := obj.Stat()
	s.Require().NoError(err)

	s.Assert().NoError(err)
	s.Assert().Equal(modelData, objData)
	s.Assert().Equal(modelVersion, stats.UserMetadata["Model_version"])
	s.Assert().Equal(modelFormat, stats.UserMetadata["Model_format"])
	s.Assert().Equal(modelDescription, stats.UserMetadata["Model_description"])
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_RegisterModel_NoDescription_ExpectOK() {
	// GIVEN
	modelData := []byte("some-data")
	modelName := "model.pt"
	modelVersion := "v1.0.0"
	modelFormat := "Pytorch"

	// WHEN
	err := s.modelRegistry.RegisterModel(
		modelData,
		modelName,
		modelVersion,
		modelFormat,
	)
	s.Assert().NoError(err)

	// THEN
	obj, err := s.client.GetObject(
		context.Background(),
		s.modelRegistryBucket,
		path.Join(
			viper.GetString(common.ConfigMinioInternalFolderKey),
			viper.GetString(common.ConfigModelFolderNameKey),
			modelName,
		),
		minio.GetObjectOptions{},
	)
	s.Require().NoError(err)

	stats, err := obj.Stat()
	s.Require().NoError(err)

	s.Assert().NoError(err)
	s.Assert().Equal(modelVersion, stats.UserMetadata["Model_version"])
	s.Assert().Equal(modelFormat, stats.UserMetadata["Model_format"])
	s.Assert().Empty(stats.UserMetadata["Model_description"])
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_RegisterModel_InvalidName_ExpectError() {
	// GIVEN
	modelData := []byte("some-data")
	modelName := ""
	modelVersion := "v1.0.0"
	modelDescription := "description"
	modelFormat := "Pytorch"

	// WHEN
	err := s.modelRegistry.RegisterModel(
		modelData,
		modelName,
		modelVersion,
		modelFormat,
		modelDescription,
	)

	// THEN
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrEmptyName)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_RegisterModel_InvalidVersion_ExpectError() {
	// GIVEN
	modelData := []byte("some-data")
	modelName := "model.pt"
	modelVersion := "version 1"
	modelDescription := "description"
	modelFormat := "Pytorch"

	// WHEN
	err := s.modelRegistry.RegisterModel(
		modelData,
		modelName,
		modelVersion,
		modelFormat,
		modelDescription,
	)

	// THEN
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrInvalidVersion)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_RegisterModel_InvalidModelPayload_ExpectError() {
	// GIVEN
	modelData := []byte("")
	modelName := "model.pt"
	modelVersion := "v1.0.0"
	modelDescription := "description"
	modelFormat := "Pytorch"

	// WHEN
	err := s.modelRegistry.RegisterModel(
		modelData,
		modelName,
		modelVersion,
		modelFormat,
		modelDescription,
	)

	// THEN
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrEmptyModel)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_RegisterModel_ModelAlreadyExists_ExpectError() {
	// GIVEN
	modelData := []byte("some-data")
	modelName := "model.pt"
	modelVersion := "v1.0.0"
	modelDescription := "description"
	modelFormat := "Pytorch"

	// WHEN
	err := s.modelRegistry.RegisterModel(
		modelData,
		modelName,
		modelVersion,
		modelFormat,
		modelDescription,
	)
	s.Assert().NoError(err)

	// THEN
	err = s.modelRegistry.RegisterModel(
		modelData,
		modelName,
		modelVersion,
		modelFormat,
		modelDescription,
	)
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrModelAlreadyExists)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_RegisterModel_NilModelPayload_ExpectError() {
	// GIVEN
	modelName := "model.pt"
	modelVersion := "v1.0.0"
	modelDescription := "description"
	modelFormat := "Pytorch"

	// WHEN
	err := s.modelRegistry.RegisterModel(
		nil,
		modelName,
		modelVersion,
		modelFormat,
		modelDescription,
	)

	// THEN
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrEmptyModel)
}
