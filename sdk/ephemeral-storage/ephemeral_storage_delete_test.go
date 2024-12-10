//go:build unit

package objectstore_test

import (
	"fmt"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"

	objectstore "github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/ephemeral-storage"

	"github.com/spf13/viper"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"
)

const (
	natsObjectStoreField = common.ConfigNatsEphemeralStorage
	natsObjectStoreValue = "ephemeral-storage"
)

func (s *SdkObjectStoreTestSuite) TestObjectStore_DeleteObjectStoreNotInitialized_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, "")

	objectStore, _ := objectstore.New(s.logger, &s.jetstream)

	// When
	err := objectStore.Delete("key")

	// Then
	s.ErrorIs(err, errors.ErrUndefinedEphemeralStorage)
	s.NotNil(objectStore)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_ErrorDeletingObject_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)
	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)
	objectStore, _ := objectstore.New(s.logger, &s.jetstream)

	s.objectStore.On("Delete", "key").Return(fmt.Errorf("error saving object"))

	// When
	err := objectStore.Delete("key")

	// Then
	s.Error(err)
	s.NotNil(objectStore)
	s.objectStore.AssertNumberOfCalls(s.T(), "Delete", 1)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_DeleteObject_ExpectOK() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)
	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)
	objectStore, _ := objectstore.New(s.logger, &s.jetstream)

	s.objectStore.On("Delete", "key").Return(nil)

	// When
	err := objectStore.Delete("key")

	// Then
	s.NoError(err)
	s.NotNil(objectStore)
	s.objectStore.AssertNumberOfCalls(s.T(), "Delete", 1)
}
