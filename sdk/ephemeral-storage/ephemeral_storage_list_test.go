//go:build unit

package objectstore_test

import (
	"fmt"

	objectStore2 "github.com/konstellation-io/kai-gosdk/sdk/ephemeral-storage"

	"github.com/spf13/viper"

	"github.com/konstellation-io/kai-gosdk/internal/errors"
)

func (s *SdkObjectStoreTestSuite) TestObjectStore_ListObjectStoreNotInitialized_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, "")

	objectStore, _ := objectStore2.New(s.logger, &s.jetstream)

	// When
	keyList, err := objectStore.List()

	// Then
	s.ErrorIs(err, errors.ErrUndefinedEphemeralStorage)
	s.Empty(keyList)
	s.NotNil(objectStore)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_ErrorListObject_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)
	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)
	objectStore, _ := objectStore2.New(s.logger, &s.jetstream)

	s.objectStore.On("List").Return(nil, fmt.Errorf("error saving object"))

	// When
	keyList, err := objectStore.List("key")

	// Then
	s.Error(err)
	s.NotNil(objectStore)
	s.Nil(keyList)
	s.objectStore.AssertNumberOfCalls(s.T(), "List", 1)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_ListObject_ExpectOK() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)
	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)
	objectStore, _ := objectStore2.New(s.logger, &s.jetstream)

	keys := []string{"key1", "key2"}
	objects := generateObjectInfoResponse(keys)

	s.objectStore.On("List").Return(objects, nil)

	// When
	keyList, err := objectStore.List()

	// Then
	s.NoError(err)
	s.NotNil(objectStore)
	s.NotNil(keyList)
	s.objectStore.AssertNumberOfCalls(s.T(), "List", 1)
	s.Equal([]string{"key1", "key2"}, keyList)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_ListObjectWithFilter_ExpectOK() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)
	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)
	objectStore, _ := objectStore2.New(s.logger, &s.jetstream)

	keys := []string{"key1", "key2", "anotherKey3", "anotherKey4"}
	objects := generateObjectInfoResponse(keys)

	s.objectStore.On("List").Return(objects, nil)

	// When
	keyList, err := objectStore.List("anotherKey*")

	// Then
	s.NoError(err)
	s.NotNil(objectStore)
	s.NotNil(keyList)
	s.objectStore.AssertNumberOfCalls(s.T(), "List", 1)
	s.Equal([]string{"anotherKey3", "anotherKey4"}, keyList)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_ListObjectWithFilter_InvalidRegexp_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)
	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)
	objectStore, _ := objectStore2.New(s.logger, &s.jetstream)

	keys := []string{"key1", "key2", "anotherKey3", "anotherKey4"}
	objects := generateObjectInfoResponse(keys)

	s.objectStore.On("List").Return(objects, nil)

	// When
	keyList, err := objectStore.List("anotherKey*[")

	// Then
	s.Error(err)
	s.NotNil(objectStore)
	s.Nil(keyList)
	s.objectStore.AssertNumberOfCalls(s.T(), "List", 1)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_ListObject_ErrorOnList_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)
	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)
	objectStore, _ := objectStore2.New(s.logger, &s.jetstream)

	s.objectStore.On("List").Return(nil, fmt.Errorf("error listing objects"))

	// When
	keyList, err := objectStore.List()

	// Then
	s.Error(err)
	s.NotNil(objectStore)
	s.Nil(keyList)
	s.objectStore.AssertNumberOfCalls(s.T(), "List", 1)
}
