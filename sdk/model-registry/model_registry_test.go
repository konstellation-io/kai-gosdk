//go:build integration

package modelregistry_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	modelregistry "github.com/konstellation-io/kai-gosdk/sdk/model-registry"

	"github.com/konstellation-io/kai-gosdk/sdk/metadata"

	"github.com/konstellation-io/kai-gosdk/internal/common"

	"github.com/go-logr/logr/testr"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type SdkModelRegistryTestSuite struct {
	suite.Suite
	modelRegistry       *modelregistry.ModelRegistry
	minioContainer      testcontainers.Container
	client              *minio.Client
	modelRegistryBucket string
}

func (s *SdkModelRegistryTestSuite) getContainer() testcontainers.Container {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio:RELEASE.2023-09-30T07-02-29Z",
		ExposedPorts: []string{"9000/tcp"},
		Cmd: []string{
			"server",
			"/data",
		},
		Env:        map[string]string{},
		WaitingFor: wait.ForLog("Status:         1 Online, 0 Offline."),
	}

	minioContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	s.Require().NoError(err)

	return minioContainer
}

func (s *SdkModelRegistryTestSuite) getClient(url, keyID, keySecret string) *minio.Client {
	client, err := minio.New(url, &minio.Options{
		Creds:        credentials.NewStaticV4(keyID, keySecret, ""),
		BucketLookup: minio.BucketLookupPath,
	})
	s.Require().NoError(err)

	return client
}

func (s *SdkModelRegistryTestSuite) SetupSuite() {
	s.minioContainer = s.getContainer()
}

func (s *SdkModelRegistryTestSuite) SetupTest() {
	// Reset viper values before each test
	viper.Reset()

	ctx := context.Background()

	s.modelRegistryBucket = "test-bucket"

	host, err := s.minioContainer.Host(ctx)
	s.Require().NoError(err)

	port, err := s.minioContainer.MappedPort(ctx, "9000/tcp")
	s.Require().NoError(err)

	minioEndpoint := fmt.Sprintf("%s:%d", host, port.Int())

	viper.Set(common.ConfigMetadataProductIDKey, "some-product")
	viper.Set(common.ConfigMetadataVersionIDKey, "some-version")
	viper.Set(common.ConfigMetadataWorkflowIDKey, "some-workflow")
	viper.Set(common.ConfigMetadataProcessIDKey, "some-process")
	viper.Set(common.ConfigMinioBucketKey, s.modelRegistryBucket)
	viper.Set(common.ConfigMinioEndpointKey, minioEndpoint)
	viper.Set(common.ConfigMinioClientUserKey, "minioadmin")
	viper.Set(common.ConfigMinioClientPasswordKey, "minioadmin")
	viper.Set(common.ConfigMinioUseSslKey, false)
	viper.Set(common.ConfigMinioInternalFolderKey, ".kai")
	viper.Set(common.ConfigModelFolderNameKey, ".models")

	s.client = s.getClient(minioEndpoint, "minioadmin", "minioadmin")

	err = s.client.MakeBucket(ctx, s.modelRegistryBucket, minio.MakeBucketOptions{})
	s.Require().NoError(err)

	err = s.client.EnableVersioning(ctx, s.modelRegistryBucket)
	s.Require().NoError(err)

	modelRegistry, err := modelregistry.NewModelRegistryIntegration(testr.New(s.T()))
	s.Assert().NoError(err)
	s.modelRegistry = modelRegistry
}

func (s *SdkModelRegistryTestSuite) TearDownTest() {
	ctx := context.Background()

	err := s.client.RemoveBucketWithOptions(
		ctx,
		s.modelRegistryBucket,
		minio.RemoveBucketOptions{ForceDelete: true},
	)
	if err != nil {
		var minioErr minio.ErrorResponse

		errors.As(err, &minioErr)

		if minioErr.StatusCode != http.StatusNotFound {
			s.Failf(err.Error(), "Error deleting bucket %q", s.modelRegistryBucket)
		}
	}
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_Initialize_ExpectOK() {
	// WHEN
	modelRegistry, err := modelregistry.New(testr.New(s.T()), metadata.New())
	s.Require().NoError(err)
	_, err = modelRegistry.ListModels()

	// THEN
	s.Assert().NoError(err)
	s.Assert().NotNil(modelRegistry)
}

func (s *SdkModelRegistryTestSuite) TestModelRegistry_Initialize_ExpectError() {
	// GIVEN
	viper.Set(common.ConfigMinioEndpointKey, "invalid-url")

	// WHEN
	modelRegistry, err := modelregistry.New(testr.New(s.T()), metadata.New())
	s.Require().NoError(err)
	objList, err := modelRegistry.ListModels()

	// THEN
	s.Assert().NoError(err)
	s.Assert().Len(objList, 0)
	s.Assert().NotNil(modelRegistry)
}

func TestSdkModelRegistryTestSuite(t *testing.T) {
	suite.Run(t, new(SdkModelRegistryTestSuite))
}

func (s *SdkModelRegistryTestSuite) registerNewModelVersion(data []byte, name, version, description, format string) error {
	return s.modelRegistry.RegisterModel(
		data,
		name,
		version,
		format,
		description,
	)
}
