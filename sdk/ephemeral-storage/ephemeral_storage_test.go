//go:build unit

package objectstore_test

import (
	"fmt"
	"testing"

	ephemeralStorage "github.com/konstellation-io/kai-gosdk/sdk/ephemeral-storage"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"

	"github.com/konstellation-io/kai-gosdk/internal/errors"
	"github.com/konstellation-io/kai-gosdk/mocks"
)

//go:generate mockery --dir $GOPATH/pkg/mod/github.com/nats-io/nats.go@v1.31.0 --output ../../mocks --name JetStreamContext --structname JetStreamContextMock --filename jetstream_context_mock.go
//go:generate mockery --dir $GOPATH/pkg/mod/github.com/nats-io/nats.go@v1.31.0 --output ../../mocks --name ObjectStore --structname NatsObjectStoreMock --filename nats_object_store_mock.go
type SdkObjectStoreTestSuite struct {
	suite.Suite
	logger      logr.Logger
	jetstream   mocks.JetStreamContextMock
	objectStore mocks.NatsObjectStoreMock
}

func (s *SdkObjectStoreTestSuite) SetupSuite() {
	s.logger = testr.NewWithOptions(s.T(), testr.Options{Verbosity: 1})
}

func (s *SdkObjectStoreTestSuite) SetupTest() {
	// Reset viper values before each test
	viper.Reset()

	s.jetstream = *mocks.NewJetStreamContextMock(s.T())
	s.objectStore = *mocks.NewNatsObjectStoreMock(s.T())
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_InitializeObjectStore_ExpectOK() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)

	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)

	// When
	objectStore, err := ephemeralStorage.New(s.logger, &s.jetstream)

	// Then
	s.NoError(err)
	s.NotNil(objectStore)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_InitializeObjectStore_EmptyOsName_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, "")

	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(&s.objectStore, nil)

	// When
	objectStore, osErr := ephemeralStorage.New(s.logger, &s.jetstream)
	_, getErr := objectStore.Get("key")

	// Then
	s.NoError(osErr)
	s.NotNil(objectStore)
	s.ErrorIs(getErr, errors.ErrUndefinedEphemeralStorage)
}

func (s *SdkObjectStoreTestSuite) TestObjectStore_InitializeObjectStore_ErrorOnOsReturn_ExpectError() {
	// Given
	viper.SetDefault(natsObjectStoreField, natsObjectStoreValue)

	s.jetstream.On("ObjectStore", natsObjectStoreValue).Return(nil, fmt.Errorf("not found"))

	// When
	objectStore, err := ephemeralStorage.New(s.logger, &s.jetstream)

	// Then
	s.Error(err)
	s.Nil(objectStore)
}

func TestSdkObjectStoreTestSuite(t *testing.T) {
	suite.Run(t, new(SdkObjectStoreTestSuite))
}

func generateObjectInfoResponse(keys []string) []*nats.ObjectInfo {
	objects := make([]*nats.ObjectInfo, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, &nats.ObjectInfo{
			ObjectMeta: nats.ObjectMeta{
				Name: key,
			},
		})
	}

	return objects
}
