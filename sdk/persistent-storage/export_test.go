//go:build integration

package persistentstorage

import (
	"fmt"

	"github.com/konstellation-io/kai-gosdk/internal/common"
	"github.com/konstellation-io/kai-gosdk/sdk/metadata"

	"github.com/go-logr/logr"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
)

func NewObject(info ObjectInfo, data []byte) Object {
	return Object{
		ObjectInfo: info,
		data:       data,
	}
}

func NewPersistentStorageIntegration(logger logr.Logger) (*PersistentStorage, error) {
	persistentStorageBucket := viper.GetString(common.ConfigMinioBucketKey)

	storageManager, err := initPersistentStorageIntegration(logger)
	if err != nil {
		return nil, err
	}

	return &PersistentStorage{
		logger:        logger,
		storageClient: storageManager,
		storageBucket: persistentStorageBucket,
		metadata:      metadata.New(),
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

	logger.Info("Successfully initialized persistent storage")

	return minioClient, nil
}
