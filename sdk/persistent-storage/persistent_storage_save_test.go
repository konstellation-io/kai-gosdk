//go:build integration

package persistentstorage_test

import (
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"
)

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_SaveObject_ExpectOK() {
	// GIVEN
	key := "some-object"
	data := []byte("some-data")

	// WHEN
	returnedVersion, err := s.persistentStorage.Save(key, data)

	// THEN
	s.Assert().NoError(err)
	s.Assert().NotEmpty(returnedVersion)
	s.Assert().Equal(key, returnedVersion.Key)
	s.Assert().NotEmpty(returnedVersion.VersionID)
	s.Assert().Empty(returnedVersion.ExpiresIn)
}

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_SaveObject_WithTTL_ExpectOK() {
	// GIVEN
	key := "some-object"
	data := []byte("some-data")
	ttlDays := 5

	// WHEN
	returnedVersion, err := s.persistentStorage.Save(key, data, ttlDays)

	// THEN
	s.Assert().NoError(err)
	s.Assert().NotEmpty(returnedVersion)
	s.Assert().Equal(key, returnedVersion.Key)
	s.Assert().NotEmpty(returnedVersion.VersionID)
	s.Assert().NotNil(returnedVersion.ExpiresIn)
}

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_SaveObjectWithNoKey_ExpectError() {
	// GIVEN
	key := ""
	data := []byte("some-data")

	// WHEN
	returnedVersion, err := s.persistentStorage.Save(key, data)

	// THEN
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrEmptyKey)
	s.Assert().Nil(returnedVersion)
}

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_SaveObjectWithNoPayload_ExpectError() {
	// GIVEN
	key := "some-key"

	// WHEN
	returnedVersion, err := s.persistentStorage.Save(key, nil)

	// THEN
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrEmptyPayload)
	s.Assert().Nil(returnedVersion)
}
