//go:build integration

package modelregistry

import (
	"fmt"
	"path"

	"github.com/konstellation-io/kai-gosdk/internal/common"
	"github.com/konstellation-io/kai-gosdk/sdk/metadata"

	"github.com/go-logr/logr"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
)

func NewModelRegistryIntegration(logger logr.Logger) (*ModelRegistry, error) {
	persistentStorageBucket := viper.GetString(common.ConfigMinioBucketKey)

	storageManager, err := initPersistentStorageIntegration(logger)
	if err != nil {
		return nil, err
	}

	return &ModelRegistry{
		logger:        logger,
		storageClient: storageManager,
		storageBucket: persistentStorageBucket,
		metadata:      metadata.New(),
		modelFolderName: path.Join(
			viper.GetString(common.ConfigMinioInternalFolderKey),
			viper.GetString(common.ConfigModelFolderNameKey),
		),
	}, nil
}

func initPersistentStorageIntegration(logger logr.Logger) (*minio.Client, error) {
	endpoint := viper.GetString(common.ConfigMinioEndpointKey)
	useSSL := viper.GetBool(common.ConfigMinioUseSslKey)
	user := viper.GetString(common.ConfigMinioClientUserKey)
	password := viper.GetString(common.ConfigMinioClientPasswordKey)

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(user, password, ""),
		Secure:       useSSL,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("error initializing persistent storage: %w", err)
	}

	logger.Info("Successfully initialized model registry storage")

	return minioClient, nil
}
