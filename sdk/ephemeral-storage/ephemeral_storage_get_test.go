//go:build unit

package objectstore_test

import (
	"fmt"

	objectstore "github.com/konstellation-io/kai-gosdk/sdk/ephemeral-storage"

	"github.com/spf13/viper"

	"github.com/konstellation-io/kai-gosdk/internal/errors"
)

func (s *SdkObjectStoreTestSuite) TestObjectStore_GetObjectStoreNotInitialized_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, "")

	objectStore, _ := objectstore.New(s.logger, &s.jetstream)

	// When
	value, err := objectStore.Get("key")

	// Then
	s.ErrorIs(err, errors.ErrUndefinedEphemeralStorage)
	s.NotNil(objectStore)
	s.Nil(value)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_ErrorRetrievingObject_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)
	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)
	objectStore, _ := objectstore.New(s.logger, &s.jetstream)

	s.objectStore.On("GetBytes", "key").Return(nil, fmt.Errorf("error saving object"))

	// When
	value, err := objectStore.Get("key")

	// Then
	s.Error(err)
	s.NotNil(objectStore)
	s.Empty(value)
	s.objectStore.AssertNumberOfCalls(s.T(), "GetBytes", 1)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_GetObject_ExpectOK() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)
	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)
	objectStore, _ := objectstore.New(s.logger, &s.jetstream)

	s.objectStore.On("GetBytes", "key").Return([]byte("value"), nil)

	// When
	value, err := objectStore.Get("key")

	// Then
	s.NoError(err)
	s.NotNil(objectStore)
	s.Equal("value", string(value))
	s.objectStore.AssertNumberOfCalls(s.T(), "GetBytes", 1)
}
