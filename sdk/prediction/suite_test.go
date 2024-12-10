//go:build integration

package prediction_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/prediction"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	_testRequestID    = "test-request-id"
	_testProduct      = "test-product"
	_testVersion      = "test-version"
	_testWorkflow     = "test-workflow"
	_testWorkflowType = "training"
	_testProcess      = "test-process"

	_testRedisUser          = "test-user"
	_testRedisPassword      = "test-password"
	_testRedisAdminPassword = "testpassword"
	_testRedisIndex         = "predictionsIdx"
)

func TestPredictionStoreSuite(t *testing.T) {
	suite.Run(t, new(PredictionStoreSuite))
}

type PredictionStoreSuite struct {
	suite.Suite
	predictionStore *prediction.RedisPredictionStore
	redisContainer  testcontainers.Container
	redisClient     *redis.Client
}

func (s *PredictionStoreSuite) SetupSuite() {
	s.initializeRedisContainer()
	s.initializeMetadataConfiguration()
	s.initializeRedisClient()
	s.createRedisUser()
	s.initializePredictionStore()
}

func (s *PredictionStoreSuite) SetupTest() {
	s.createRedisPredictionIndex()
}

func (s *PredictionStoreSuite) TearDownTest() {
	err := s.redisClient.FlushAll(context.Background()).Err()
	s.Require().NoError(err)
}

func (s *PredictionStoreSuite) initializeRedisContainer() {
	ctx := context.Background()
	exposedPort := "6379/tcp"

	testdataPath, err := filepath.Abs("./testdata")
	s.Require().NoError(err)

	redisConfContainerPath := "/usr/local/etc/redis"

	req := testcontainers.ContainerRequest{
		Image:        "redis/redis-stack-server:7.2.0-v6",
		ExposedPorts: []string{exposedPort},
		WaitingFor:   wait.ForLog("Ready to accept connections tcp"),
		Mounts: []testcontainers.ContainerMount{
			{
				Source: testcontainers.DockerBindMountSource{
					HostPath: testdataPath,
				},
				Target: testcontainers.ContainerMountTarget(redisConfContainerPath),
			},
		},
	}

	s.redisContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	s.Require().NoError(err)

	redisEndpoint, err := s.redisContainer.PortEndpoint(context.Background(), nat.Port(exposedPort), "")
	s.Require().NoError(err)

	viper.Set(common.ConfigRedisEndpointKey, redisEndpoint)
}

func (s *PredictionStoreSuite) initializeMetadataConfiguration() {
	viper.Set(common.ConfigMetadataProductIDKey, _testProduct)
	viper.Set(common.ConfigMetadataVersionIDKey, _testVersion)
	viper.Set(common.ConfigMetadataWorkflowIDKey, _testWorkflow)
	viper.Set(common.ConfigMetadataWorkflowTypeKey, _testWorkflowType)
	viper.Set(common.ConfigMetadataProcessIDKey, _testProcess)
	viper.Set(common.ConfigRedisUsernameKey, _testRedisUser)
	viper.Set(common.ConfigRedisPasswordKey, _testRedisPassword)
	viper.Set(common.ConfigRedisIndexKey, _testRedisIndex)
}

func (s *PredictionStoreSuite) initializePredictionStore() {
	s.predictionStore = prediction.NewRedisPredictionStore(_testRequestID)
}

func (s *PredictionStoreSuite) getClient(url, keyID, keySecret string) *minio.Client {
	redisClient, err := minio.New(url, &minio.Options{
		Creds:        credentials.NewStaticV4(keyID, keySecret, ""),
		BucketLookup: minio.BucketLookupPath,
	})
	s.Require().NoError(err)

	return redisClient
}

func (s *PredictionStoreSuite) initializeRedisClient() {
	opts := &redis.Options{
		Addr:     viper.GetString(common.ConfigRedisEndpointKey),
		Username: "default",
		Password: _testRedisAdminPassword,
	}
	s.redisClient = redis.NewClient(opts)
}

func (s *PredictionStoreSuite) createRedisUser() {
	var (
		allowedKeys = fmt.Sprintf("~%s:*", _testProduct)
		passwordArg = fmt.Sprintf(">%s", _testRedisPassword)
	)

	command := s.redisClient.Do(
		context.Background(),
		"ACL", "SETUSER", _testRedisUser, allowedKeys, passwordArg, "+@all", "+ft.search", "~predictionsIdx", "-@dangerous", "on",
	)
	s.Require().NoError(command.Err())
}

func (s *PredictionStoreSuite) createRedisPredictionIndex() {
	err := s.redisClient.Do(context.Background(),
		"FT.CREATE",
		_testRedisIndex,
		"ON",
		"JSON",
		"SCHEMA",
		"$.metadata.product", "AS", "product", "TAG",
		"$.metadata.version", "AS", "version", "TAG",
		"$.metadata.workflow", "AS", "workflow", "TAG",
		"$.metadata.workflow_type", "AS", "workflow_type", "TAG",
		"$.metadata.process", "AS", "process", "TAG",
		"$.metadata.request_id", "AS", "request_id", "TAG",
		"$.creation_date", "AS", "creation_date", "NUMERIC",
	).Err()
	s.Require().NoError(err)
}

func (s *PredictionStoreSuite) getPredictionMetadata() prediction.Metadata {
	return prediction.Metadata{
		Product:      viper.GetString(common.ConfigMetadataProductIDKey),
		Version:      viper.GetString(common.ConfigMetadataVersionIDKey),
		Workflow:     viper.GetString(common.ConfigMetadataWorkflowIDKey),
		WorkflowType: viper.GetString(common.ConfigMetadataWorkflowTypeKey),
		Process:      viper.GetString(common.ConfigMetadataProcessIDKey),
		RequestID:    _testRequestID,
	}
}

func (s *PredictionStoreSuite) getKeyWithProductPrefix(predictionID string) string {
	return fmt.Sprintf("%s:%s", _testProduct, predictionID)
}
