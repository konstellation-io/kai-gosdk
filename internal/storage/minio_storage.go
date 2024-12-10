package storage

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/auth"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
)

const (
	_storageLoggerName = "[STORAGE]"
)

type Storage struct {
	logger logr.Logger
}

func New(logger logr.Logger) *Storage {
	return &Storage{
		logger: logger,
	}
}

func (s *Storage) GetStorageClient() (*minio.Client, error) {
	endpoint := viper.GetString(common.ConfigMinioEndpointKey)
	useSSL := viper.GetBool(common.ConfigMinioUseSslKey)
	url := ""

	if useSSL {
		url = fmt.Sprintf("%s://%s", "https", viper.GetString(common.ConfigMinioEndpointKey))
	} else {
		url = fmt.Sprintf("%s://%s", "http", viper.GetString(common.ConfigMinioEndpointKey))
	}

	minioCredentials, err := s.getClientCredentials(url)
	if err != nil {
		return nil, fmt.Errorf("error initializing persistent storage: %w", err)
	}

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:        minioCredentials,
		Secure:       useSSL,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("error initializing persistent storage: %w", err)
	}

	s.logger.WithName(_storageLoggerName).V(2).Info("Successfully initialized storage client")

	return minioClient, nil
}

func (s *Storage) getClientCredentials(url string) (*credentials.Credentials, error) {
	minioCreds, err := credentials.NewSTSClientGrants(
		url,
		func() (*credentials.ClientGrantsToken, error) {
			authClient := auth.New(s.logger)

			token, err := authClient.GetToken()
			if err != nil {
				return nil, err
			}

			return &credentials.ClientGrantsToken{
				Token:  token.AccessToken,
				Expiry: token.ExpiresIn,
			}, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return minioCreds, err
}
