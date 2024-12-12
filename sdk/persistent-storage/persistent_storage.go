package persistentstorage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/konstellation-io/kai-gosdk/internal/storage"

	"github.com/konstellation-io/kai-gosdk/sdk/metadata"
	"github.com/minio/minio-go/v7/pkg/lifecycle"

	"github.com/go-logr/logr"
	"github.com/konstellation-io/kai-gosdk/internal/common"
	"github.com/konstellation-io/kai-gosdk/internal/errors"
	"github.com/minio/minio-go/v7"
	"github.com/spf13/viper"
)

const (
	_persistentStorageLoggerName = "[PERSISTENT STORAGE]"

	_productMetadata  = "product"
	_versionMetadata  = "version"
	_workflowMetadata = "workflow"
	_processMetadata  = "process"
)

type PersistentStorage struct {
	logger        logr.Logger
	storageClient *minio.Client
	storageBucket string
	metadata      *metadata.Metadata
}

type ObjectInfo struct {
	Key       string
	VersionID string
	ExpiresIn time.Time
}

type Object struct {
	ObjectInfo
	data []byte
}

func (o Object) GetAsString() string {
	return string(o.data)
}

func (o Object) GetBytes() []byte {
	return o.data
}

func New(logger logr.Logger, meta *metadata.Metadata) (*PersistentStorage, error) {
	persistentStorageBucket := viper.GetString(common.ConfigMinioBucketKey)

	storageClient, err := storage.New(logger).GetStorageClient()
	if err != nil {
		return nil, err
	}

	return &PersistentStorage{
		logger:        logger,
		storageClient: storageClient,
		storageBucket: persistentStorageBucket,
		metadata:      meta,
	}, nil
}

func (ps PersistentStorage) Save(key string, payload []byte, ttlDays ...int) (*ObjectInfo, error) {
	ctx := context.Background()

	if key == "" {
		return nil, errors.ErrEmptyKey
	}

	if strings.HasPrefix(key, viper.GetString(common.ConfigMinioInternalFolderKey)) {
		return nil, errors.ErrInvalidKey
	}

	if len(payload) == 0 {
		return nil, errors.ErrEmptyPayload
	}

	err := ps.addLifecycleDeletionRule(ctx, key, ttlDays)
	if err != nil {
		return nil, fmt.Errorf("error adding lifecycle deletion rule: %w", err)
	}

	reader := bytes.NewReader(payload)

	opts := minio.PutObjectOptions{
		UserMetadata: map[string]string{
			_productMetadata:  ps.metadata.GetProduct(),
			_versionMetadata:  ps.metadata.GetVersion(),
			_workflowMetadata: ps.metadata.GetWorkflow(),
			_processMetadata:  ps.metadata.GetProcess(),
		},
	}

	info, err := ps.storageClient.PutObject(
		ctx,
		ps.storageBucket,
		key,
		reader,
		int64(reader.Len()),
		opts,
	)
	if err != nil {
		return nil, fmt.Errorf("error storing object to the persistent storage: %w", err)
	}

	ps.logger.WithName(_persistentStorageLoggerName).V(1).
		Info(fmt.Sprintf("Object %s successfully stored in persistent storage with version ID %s",
			key, info.VersionID))

	obj := &ObjectInfo{
		Key:       key,
		VersionID: info.VersionID,
		ExpiresIn: info.Expiration,
	}

	return obj, nil
}

func (ps PersistentStorage) Get(key string, version ...string) (*Object, error) {
	if key == "" {
		return nil, errors.ErrEmptyKey
	}

	if strings.HasPrefix(key, viper.GetString(common.ConfigMinioInternalFolderKey)) {
		return nil, errors.ErrInvalidKey
	}

	opts := minio.GetObjectOptions{}

	if len(version) > 0 && version[0] != "" {
		opts = minio.GetObjectOptions{
			VersionID: version[0],
		}
	}

	object, err := ps.storageClient.GetObject(
		context.Background(),
		ps.storageBucket,
		key,
		opts,
	)
	if err != nil {
		return nil, fmt.Errorf("error retrieving object from the persistent storage: %w", err)
	}

	ps.logger.WithName(_persistentStorageLoggerName).V(1).
		Info(fmt.Sprintf("Object %s successfully retrieved from persistent storage", key))

	defer object.Close()

	data, err := io.ReadAll(object)
	if err != nil {
		return nil, fmt.Errorf("error reading object from the persistent storage: %w", err)
	}

	objStats, err := object.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting object stats from the persistent storage: %w", err)
	}

	obj := &Object{
		ObjectInfo: ObjectInfo{
			Key:       key,
			VersionID: objStats.VersionID,
			ExpiresIn: objStats.Expiration,
		},
		data: data,
	}

	return obj, nil
}

func (ps PersistentStorage) List() ([]*ObjectInfo, error) {
	var objectList []*ObjectInfo

	objects := ps.storageClient.ListObjects(
		context.Background(),
		ps.storageBucket,
		minio.ListObjectsOptions{
			WithMetadata: true,
			Recursive:    true,
		},
	)

	ps.logger.WithName(_persistentStorageLoggerName).V(1).
		Info("Objects successfully retrieved from persistent storage")

	for object := range objects {
		if object.Key != "" && !strings.HasPrefix(object.Key, viper.GetString(common.ConfigMinioInternalFolderKey)) {
			stats, err := ps.storageClient.StatObject(context.Background(), ps.storageBucket, object.Key, minio.StatObjectOptions{})
			if err != nil {
				return nil, fmt.Errorf("error getting object stats from the persistent storage: %w", err)
			}

			objectList = append(
				objectList,
				&ObjectInfo{
					Key:       object.Key,
					VersionID: stats.VersionID,
					ExpiresIn: stats.Expiration,
				},
			)
		}
	}

	return objectList, nil
}

func (ps PersistentStorage) ListVersions(key string) ([]*ObjectInfo, error) {
	var objectList []*ObjectInfo

	if key == "" {
		return nil, errors.ErrEmptyKey
	}

	objects := ps.storageClient.ListObjects(
		context.Background(),
		ps.storageBucket,
		minio.ListObjectsOptions{
			WithVersions: true,
			WithMetadata: true,
			Recursive:    true,
			Prefix:       key,
		},
	)

	ps.logger.WithName(_persistentStorageLoggerName).V(1).
		Info(fmt.Sprintf("Object versions successfully retrieved for prefix %s from persistent storage", key))

	for object := range objects {
		if object.VersionID != "" && !strings.HasPrefix(object.Key, viper.GetString(common.ConfigMinioInternalFolderKey)) {
			objectList = append(
				objectList,
				&ObjectInfo{
					Key:       object.Key,
					VersionID: object.VersionID,
					ExpiresIn: object.Expiration,
				},
			)
		}
	}

	return objectList, nil
}

func (ps PersistentStorage) Delete(key string, version ...string) error {
	if key == "" {
		return errors.ErrEmptyKey
	}

	if strings.HasPrefix(key, viper.GetString(common.ConfigMinioInternalFolderKey)) {
		return errors.ErrInvalidKey
	}

	opts := minio.RemoveObjectOptions{
		ForceDelete: true,
	}

	if len(version) > 0 && version[0] != "" {
		opts.VersionID = version[0]
	}

	err := ps.storageClient.RemoveObject(
		context.Background(),
		ps.storageBucket,
		key,
		opts,
	)
	if err != nil {
		return fmt.Errorf("error deleting object from the persistent storage: %w", err)
	}

	ps.logger.WithName(_persistentStorageLoggerName).
		V(1).Info(fmt.Sprintf("Object %s successfully deleted from persistent storage", key))

	return nil
}

func (ps PersistentStorage) addLifecycleDeletionRule(ctx context.Context, key string, ttlDays []int) error {
	if len(ttlDays) > 0 && ttlDays[0] > 0 {
		lc, err := ps.storageClient.GetBucketLifecycle(ctx, ps.storageBucket)
		if err != nil {
			lc = lifecycle.NewConfiguration()
		}

		rule := lifecycle.Rule{
			ID:     fmt.Sprintf("ttl-%s", key),
			Status: minio.Enabled,
			RuleFilter: lifecycle.Filter{
				Prefix: key,
			},
			Expiration: lifecycle.Expiration{
				Days: lifecycle.ExpirationDays(ttlDays[0]),
			},
		}

		found := false

		for _, r := range lc.Rules {
			if r.ID == rule.ID {
				found = true
				break
			}
		}

		if !found {
			lc.Rules = append(lc.Rules, rule)
		}

		err = ps.storageClient.SetBucketLifecycle(ctx, ps.storageBucket, lc)
		if err != nil {
			return err
		}
	}

	return nil
}
