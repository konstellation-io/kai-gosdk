package modelregistry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"

	"github.com/Masterminds/semver/v3"
	"github.com/go-logr/logr"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/storage"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/metadata"
	"github.com/minio/minio-go/v7"
	"github.com/spf13/viper"
)

const (
	_modelRegistryLoggerName = "[MODEL REGISTRY]"
	_errorRetrievingModel    = "error reading model from the model registry: %w"

	_productMetadata          = "Product"
	_versionMetadata          = "Version"
	_workflowMetadata         = "Workflow"
	_processMetadata          = "Process"
	_modelVersionMetadata     = "Model_version"
	_modelFormatMetadata      = "Model_format"
	_modelDescriptionMetadata = "Model_description"
)

type ModelRegistry struct {
	logger          logr.Logger
	storageClient   *minio.Client
	storageBucket   string
	metadata        *metadata.Metadata
	modelFolderName string
}

type ModelInfo struct {
	Name        string
	Version     string
	Description string
	Format      string
}

type Model struct {
	ModelInfo
	Model []byte
}

func New(logger logr.Logger, meta *metadata.Metadata) (*ModelRegistry, error) {
	persistentStorageBucket := viper.GetString(common.ConfigMinioBucketKey)

	modelFolder := path.Join(
		viper.GetString(common.ConfigMinioInternalFolderKey),
		viper.GetString(common.ConfigModelFolderNameKey),
	)

	storageClient, err := storage.New(logger).GetStorageClient()
	if err != nil {
		return nil, err
	}

	return &ModelRegistry{
		logger:          logger,
		storageClient:   storageClient,
		storageBucket:   persistentStorageBucket,
		metadata:        meta,
		modelFolderName: modelFolder,
	}, nil
}

func (mr *ModelRegistry) RegisterModel(model []byte, name, version, modelFormat string, description ...string) error {
	ctx := context.Background()

	if name == "" {
		return errors.ErrEmptyName
	}

	modelDescription := ""
	if len(description) > 0 {
		modelDescription = description[0]
	}

	if _, err := semver.NewVersion(version); err != nil {
		return errors.ErrInvalidVersion
	}

	if len(model) == 0 {
		return errors.ErrEmptyModel
	}

	model_, err := mr.GetModel(name, version)
	if err == nil && model_ != nil {
		return errors.ErrModelAlreadyExists
	}

	reader := bytes.NewReader(model)

	opts := minio.PutObjectOptions{
		UserMetadata: map[string]string{
			_productMetadata:          mr.metadata.GetProduct(),
			_versionMetadata:          mr.metadata.GetVersion(),
			_workflowMetadata:         mr.metadata.GetWorkflow(),
			_processMetadata:          mr.metadata.GetProcess(),
			_modelFormatMetadata:      modelFormat,
			_modelDescriptionMetadata: modelDescription,
			_modelVersionMetadata:     version,
		},
	}

	_, err = mr.storageClient.PutObject(
		ctx,
		mr.storageBucket,
		mr.getModelPath(name),
		reader,
		int64(reader.Len()),
		opts,
	)
	if err != nil {
		return fmt.Errorf("error storing model to the model registry: %w", err)
	}

	mr.logger.WithName(_modelRegistryLoggerName).V(1).
		Info(fmt.Sprintf("Model %s successfully stored to the model registry with version %s", name, version))

	return nil
}

func (mr *ModelRegistry) GetModel(name string, version ...string) (*Model, error) {
	if name == "" {
		return nil, errors.ErrEmptyName
	}

	opts := minio.GetObjectOptions{}

	if len(version) > 0 {
		if _, err := semver.NewVersion(version[0]); err == nil {
			return mr.getModelVersionFromList(name, version[0])
		}

		return nil, errors.ErrInvalidVersion
	}

	return mr.getModelVersion(name, opts)
}

func (mr *ModelRegistry) ListModels() ([]*ModelInfo, error) {
	var modelInfoList []*ModelInfo

	objects := mr.storageClient.ListObjects(
		context.Background(),
		mr.storageBucket,
		minio.ListObjectsOptions{
			WithMetadata: true,
			Recursive:    true,
			Prefix:       mr.getModelPath(""),
		},
	)

	mr.logger.WithName(_modelRegistryLoggerName).V(1).
		Info("Model successfully retrieved from model registry")

	for object := range objects {
		if object.Key != "" {
			stats, err := mr.storageClient.StatObject(context.Background(), mr.storageBucket, object.Key, minio.StatObjectOptions{})
			if err != nil {
				return nil, fmt.Errorf("error getting model stats from the model registry: %w", err)
			}

			modelInfoList = append(
				modelInfoList,
				&ModelInfo{
					Name:        mr.getModelName(object.Key),
					Version:     stats.UserMetadata[_modelVersionMetadata],
					Description: stats.UserMetadata[_modelDescriptionMetadata],
					Format:      stats.UserMetadata[_modelFormatMetadata],
				},
			)
		}
	}

	return modelInfoList, nil
}

func (mr *ModelRegistry) ListModelVersions(name string) ([]*ModelInfo, error) {
	var modelInfoList []*ModelInfo

	if name == "" {
		return nil, errors.ErrEmptyName
	}

	objects := mr.storageClient.ListObjects(
		context.Background(),
		mr.storageBucket,
		minio.ListObjectsOptions{
			WithVersions: true,
			WithMetadata: true,
			Recursive:    true,
			Prefix:       mr.getModelPath(name),
		},
	)

	mr.logger.WithName(_modelRegistryLoggerName).V(1).
		Info(fmt.Sprintf("Model versions successfully retrieved for prefix %s from model registry", name))

	for object := range objects {
		if object.VersionID != "" {
			stats, err := mr.storageClient.StatObject(context.Background(), mr.storageBucket, object.Key, minio.StatObjectOptions{
				VersionID: object.VersionID,
			})
			if err != nil {
				return nil, fmt.Errorf("error getting model stats from the model registry: %w", err)
			}

			modelInfoList = append(
				modelInfoList,
				&ModelInfo{
					Name:        mr.getModelName(object.Key),
					Version:     stats.UserMetadata[_modelVersionMetadata],
					Description: stats.UserMetadata[_modelDescriptionMetadata],
					Format:      stats.UserMetadata[_modelFormatMetadata],
				},
			)
		}
	}

	return modelInfoList, nil
}

func (mr *ModelRegistry) DeleteModel(name string) error {
	if name == "" {
		return errors.ErrEmptyName
	}

	opts := minio.RemoveObjectOptions{
		ForceDelete: true,
	}

	err := mr.storageClient.RemoveObject(
		context.Background(),
		mr.storageBucket,
		mr.getModelPath(name),
		opts,
	)
	if err != nil {
		return fmt.Errorf("error deleting model from the model registry: %w", err)
	}

	mr.logger.WithName(_modelRegistryLoggerName).
		V(1).Info(fmt.Sprintf("Model %s successfully deleted from model registry", name))

	return nil
}

func (mr *ModelRegistry) getModelPath(modelName string) string {
	return path.Join(mr.modelFolderName, modelName)
}

func (mr *ModelRegistry) getModelName(fullPath string) string {
	_, fileName := path.Split(fullPath)

	return fileName
}

func (mr *ModelRegistry) getModelVersion(name string, opts minio.GetObjectOptions) (*Model, error) {
	// Retrieve latest model version
	object, err := mr.storageClient.GetObject(
		context.Background(),
		mr.storageBucket,
		mr.getModelPath(name),
		opts,
	)
	if err != nil {
		return nil, errors.ErrModelNotFound
	}

	// Get model data as bytes
	modelData, err := io.ReadAll(object)
	if err != nil {
		return nil, errors.ErrModelNotFound
	}

	// Get model metadata from the object stats
	modelStats, err := object.Stat()
	if err != nil {
		return nil, fmt.Errorf("error reading model from the model registry: %w", err)
	}

	return &Model{
		ModelInfo: ModelInfo{
			Name:        mr.getModelName(modelStats.Key),
			Version:     modelStats.UserMetadata[_modelVersionMetadata],
			Description: modelStats.UserMetadata[_modelDescriptionMetadata],
			Format:      modelStats.UserMetadata[_modelFormatMetadata],
		},
		Model: modelData,
	}, nil
}

func (mr *ModelRegistry) getModelVersionFromList(name, version string) (*Model, error) {
	objectList := mr.storageClient.ListObjects(
		context.Background(),
		mr.storageBucket,
		minio.ListObjectsOptions{
			Prefix:       mr.getModelPath(name),
			WithVersions: true,
			Recursive:    false,
		},
	)

	for object := range objectList {
		if object.Err != nil {
			return nil, errors.ErrModelNotFound
		}

		stats, err := mr.storageClient.StatObject(
			context.Background(),
			mr.storageBucket,
			object.Key,
			minio.StatObjectOptions{
				VersionID: object.VersionID,
			},
		)
		if err != nil {
			return nil, fmt.Errorf(_errorRetrievingModel, err)
		}

		// Check if the object is the one we are looking for and not a directory
		if object.Key == mr.getModelPath(name) && stats.UserMetadata[_modelVersionMetadata] == version {
			objectData, err := mr.storageClient.GetObject(
				context.Background(),
				mr.storageBucket,
				object.Key,
				minio.GetObjectOptions{
					VersionID: object.VersionID,
				},
			)
			if err != nil {
				return nil, fmt.Errorf(_errorRetrievingModel, err)
			}

			// Get model data as bytes
			modelData, err := io.ReadAll(objectData)
			if err != nil {
				return nil, fmt.Errorf(_errorRetrievingModel, err)
			}

			return &Model{
				ModelInfo: ModelInfo{
					Name:        mr.getModelName(object.Key),
					Version:     stats.UserMetadata[_modelVersionMetadata],
					Description: stats.UserMetadata[_modelDescriptionMetadata],
					Format:      stats.UserMetadata[_modelFormatMetadata],
				},
				Model: modelData,
			}, nil
		}
	}

	return nil, errors.ErrModelNotFound
}
