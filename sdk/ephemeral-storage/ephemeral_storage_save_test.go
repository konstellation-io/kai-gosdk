//go:build unit

package objectstore_test

import (
	"fmt"

	objectstore "github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/ephemeral-storage"

	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"
)

func (s *SdkObjectStoreTestSuite) TestObjectStore_SaveObjectStoreNotInitialized_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, "")

	objectStore, err := objectstore.New(s.logger, &s.jetstream)
	s.NoError(err)

	// When
	err = objectStore.Save("key", []byte("value"))

	// Then
	s.ErrorIs(err, errors.ErrUndefinedEphemeralStorage)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_SaveEmptyPayload_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)

	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)

	objectStore, err := objectstore.New(s.logger, &s.jetstream)
	s.NoError(err)

	s.objectStore.On("GetInfo", "key").Return(&nats.ObjectInfo{},
		fmt.Errorf("object does not exist"))
	s.objectStore.On("PutBytes", "key", []byte("value")).Return(&nats.ObjectInfo{}, nil)

	// When
	err = objectStore.Save("key", nil)

	// Then
	s.ErrorIs(err, errors.ErrEmptyPayload)
	s.objectStore.AssertNotCalled(s.T(), "PutBytes")
	s.objectStore.AssertNotCalled(s.T(), "GetInfo")
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_ErrorSavingPayload_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)

	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)

	objectStore, err := objectstore.New(s.logger, &s.jetstream)
	s.NoError(err)

	s.objectStore.On("GetInfo", "key").Return(&nats.ObjectInfo{},
		fmt.Errorf("object does not exist"))
	s.objectStore.On("PutBytes", "key", []byte("value")).Return(nil, fmt.Errorf("error saving payload"))

	// When
	err = objectStore.Save("key", []byte("value"))

	// Then
	s.Error(err)
	s.objectStore.AssertNumberOfCalls(s.T(), "PutBytes", 1)
	s.objectStore.AssertNumberOfCalls(s.T(), "GetInfo", 1)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_SaveObject_ExpectOK() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)

	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)

	objectStore, err := objectstore.New(s.logger, &s.jetstream)
	s.NoError(err)

	s.objectStore.On("GetInfo", "key").Return(&nats.ObjectInfo{},
		fmt.Errorf("object does not exist"))
	s.objectStore.On("PutBytes", "key", []byte("value")).Return(&nats.ObjectInfo{}, nil)

	// When
	err = objectStore.Save("key", []byte("value"))

	// Then
	s.NoError(err)
	s.objectStore.AssertNumberOfCalls(s.T(), "PutBytes", 1)
	s.objectStore.AssertNumberOfCalls(s.T(), "GetInfo", 1)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_SaveDuplicatedObject_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)

	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)

	objectStore, err := objectstore.New(s.logger, &s.jetstream)
	s.NoError(err)

	s.objectStore.On("GetInfo", "key").Return(&nats.ObjectInfo{}, nil)
	s.objectStore.On("PutBytes", "key", []byte("value")).Return(&nats.ObjectInfo{}, nil)

	// When
	err = objectStore.Save("key", []byte("value"))

	// Then
	s.Error(err)
	s.ErrorIs(err, errors.ErrObjectAlreadyExists)
	s.objectStore.AssertNotCalled(s.T(), "PutBytes")
	s.objectStore.AssertNumberOfCalls(s.T(), "GetInfo", 1)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_SaveDuplicatedObject_Overwrite_ExpectOk() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)

	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)

	objectStore, _ := objectstore.New(s.logger, &s.jetstream)

	s.objectStore.On("GetInfo", "key").Return(&nats.ObjectInfo{}, nil)
	s.objectStore.On("PutBytes", "key", []byte("value")).Return(&nats.ObjectInfo{}, nil)

	// When
	err := objectStore.Save("key", []byte("value"), true)

	// Then
	s.NoError(err)
	s.NotNil(objectStore)
	s.objectStore.AssertNumberOfCalls(s.T(), "PutBytes", 1)
	s.objectStore.AssertNumberOfCalls(s.T(), "GetInfo", 1)
}
