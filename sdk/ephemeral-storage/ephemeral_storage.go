package objectstore

import (
	"fmt"
	regexp2 "regexp"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"
)

const (
	_ephemeralStorageLoggerName = "[EPHEMERAL STORAGE]"
)

type EphemeralStorage struct {
	logger                 logr.Logger
	ephemeralStorage       nats.ObjectStore
	ephemeralStorageBucket string
}

func New(logger logr.Logger, jetstream nats.JetStreamContext) (*EphemeralStorage, error) {
	ephemeralStorageBucket := viper.GetString(common.ConfigNatsEphemeralStorage)

	ephemeralStorage, err := initEphemeralStorageDeps(logger, jetstream, ephemeralStorageBucket)
	if err != nil {
		return nil, err
	}

	return &EphemeralStorage{
		logger:                 logger,
		ephemeralStorage:       ephemeralStorage,
		ephemeralStorageBucket: ephemeralStorageBucket,
	}, nil
}

func initEphemeralStorageDeps(logger logr.Logger, jetstream nats.JetStreamContext,
	objectStoreName string) (nats.ObjectStore, error) {
	if objectStoreName != "" {
		objStore, err := jetstream.ObjectStore(objectStoreName)
		if err != nil {
			return nil, fmt.Errorf("error initializing the ephemeral storage with name %s: %w", objectStoreName, err)
		}

		return objStore, nil
	}

	logger.WithName(_ephemeralStorageLoggerName).
		Info("Ephemeral storage name is not defined. Skipping ephemeral storage initialization.")

	return nil, nil
}

func (es EphemeralStorage) Save(key string, payload []byte, overwrite ...bool) error {
	overwriteValue := false
	if len(overwrite) > 0 {
		overwriteValue = overwrite[0]
	}

	if es.ephemeralStorage == nil {
		return errors.ErrUndefinedEphemeralStorage
	}

	if len(payload) == 0 {
		return errors.ErrEmptyPayload
	}

	if _, err := es.ephemeralStorage.GetInfo(key); err == nil && !overwriteValue {
		return errors.ErrObjectAlreadyExists
	}

	_, err := es.ephemeralStorage.PutBytes(key, payload)
	if err != nil {
		return fmt.Errorf("error storing object to the ephemeral storage with name %s: %w", es.ephemeralStorageBucket, err)
	}

	es.logger.WithName(_ephemeralStorageLoggerName).V(1).
		Info(fmt.Sprintf("File successfully stored in the ephemeral storage with name %s", es.ephemeralStorageBucket))

	return nil
}

func (es EphemeralStorage) Get(key string) ([]byte, error) {
	if es.ephemeralStorage == nil {
		return nil, errors.ErrUndefinedEphemeralStorage
	}

	response, err := es.ephemeralStorage.GetBytes(key)
	if err != nil {
		return nil, fmt.Errorf("error retrieving object for key %s from "+
			"the ephemeral storage with name %s: %w", key, es.ephemeralStorageBucket, err)
	}

	es.logger.WithName(_ephemeralStorageLoggerName).V(1).
		Info(fmt.Sprintf("File successfully for key %s retrieved from the "+
			"ephemeral storage with name %s", key, es.ephemeralStorageBucket))

	return response, nil
}

func (es EphemeralStorage) List(regexp ...string) ([]string, error) {
	if es.ephemeralStorage == nil {
		return nil, errors.ErrUndefinedEphemeralStorage
	}

	objStoreList, err := es.ephemeralStorage.List()
	if err != nil {
		return nil, fmt.Errorf("error listing objects from the ephemeral storage with name %s: %w", es.ephemeralStorageBucket, err)
	}

	var pattern *regexp2.Regexp

	if len(regexp) > 0 && regexp[0] != "" {
		pat, err := regexp2.Compile(regexp[0])
		if err != nil {
			return nil, fmt.Errorf("error compiling regexp: %w", err)
		}

		pattern = pat
	}

	var response []string

	for _, objName := range objStoreList {
		if pattern == nil || pattern.MatchString(objName.Name) {
			response = append(response, objName.Name)
		}
	}

	es.logger.WithName(_ephemeralStorageLoggerName).V(2).
		Info(fmt.Sprintf("Files successfully listed from the ephemeral storage with name %s", es.ephemeralStorageBucket))

	return response, nil
}

func (es EphemeralStorage) Delete(key string) error {
	if es.ephemeralStorage == nil {
		return errors.ErrUndefinedEphemeralStorage
	}

	err := es.ephemeralStorage.Delete(key)
	if err != nil {
		return fmt.Errorf("error retrieving object with key %s from the ephemeral storage with name %s: %w", key, es.ephemeralStorageBucket, err)
	}

	es.logger.WithName(_ephemeralStorageLoggerName).V(1).
		Info(fmt.Sprintf("File successfully deleted for key "+
			"%s in the ephemeral storage with name %s", key, es.ephemeralStorageBucket))

	return nil
}

func (es EphemeralStorage) Purge(regexp ...string) error {
	if es.ephemeralStorage == nil {
		return errors.ErrUndefinedEphemeralStorage
	}

	var pattern *regexp2.Regexp

	if len(regexp) > 0 && regexp[0] != "" {
		pat, err := regexp2.Compile(regexp[0])
		if err != nil {
			return fmt.Errorf("error compiling regexp: %w", err)
		}

		pattern = pat
	}

	objects, err := es.List()
	if err != nil {
		return fmt.Errorf("error listing objects from the ephemeral storage with name %s: %w", es.ephemeralStorageBucket, err)
	}

	for _, objectName := range objects {
		if pattern == nil || pattern.MatchString(objectName) {
			es.logger.WithName(_ephemeralStorageLoggerName).V(1).
				Info(fmt.Sprintf("Deleting object with key %s from the"+
					" ephemeral storage with name %s", objectName, es.ephemeralStorageBucket))

			err := es.ephemeralStorage.Delete(objectName)
			if err != nil {
				return fmt.Errorf("error purging objects from the ephemeral storage with name %s: %w", es.ephemeralStorageBucket, err)
			}
		}
	}

	es.logger.WithName(_ephemeralStorageLoggerName).V(2).
		Info(fmt.Sprintf("Files successfully purged from the ephemeral storage with name %s", es.ephemeralStorageBucket))

	return nil
}
