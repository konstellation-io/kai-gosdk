//go:build integration

package persistentstorage_test

import (
	"bytes"
	"context"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"
	"github.com/minio/minio-go/v7"
)

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_GetObject_ExpectOK() {
	// GIVEN
	key := "some-object"
	data := []byte("some-data")
	_, err := s.client.PutObject(
		context.Background(),
		s.persistentStorageBucket,
		key,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{},
	)
	s.Require().NoError(err)

	// WHEN
	returnedVersion, err := s.persistentStorage.Get(key)

	// THEN
	s.Assert().NoError(err)
	s.Assert().NotEmpty(returnedVersion)
	s.Assert().Equal(key, returnedVersion.Key)
	s.Assert().NotEmpty(returnedVersion.VersionID)
	s.Assert().Equal(data, returnedVersion.GetBytes())
}

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_GetObjectWithVersion_ExpectOK() {
	// GIVEN
	key := "some-object"
	data := []byte("some-data")
	data2 := []byte("some-data2")
	obj, err := s.client.PutObject(
		context.Background(),
		s.persistentStorageBucket,
		key,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{},
	)
	s.Require().NoError(err)
	obj2, err := s.client.PutObject(
		context.Background(),
		s.persistentStorageBucket,
		key,
		bytes.NewReader(data2),
		int64(len(data2)),
		minio.PutObjectOptions{},
	)
	s.Require().NoError(err)

	// WHEN
	returnedVersion, err := s.persistentStorage.Get(key, obj.VersionID)
	returnedVersion2, err2 := s.persistentStorage.Get(key, obj2.VersionID)

	// THEN
	s.Assert().NoError(err)
	s.Assert().NoError(err2)
	s.Assert().NotEmpty(returnedVersion)
	s.Assert().NotEmpty(returnedVersion2)
	s.Assert().Equal(key, returnedVersion.Key)
	s.Assert().Equal(key, returnedVersion2.Key)
	s.Assert().NotEmpty(returnedVersion.VersionID)
	s.Assert().NotEmpty(returnedVersion2.VersionID)
	s.Assert().Equal(data, returnedVersion.GetBytes())
	s.Assert().Equal(data2, returnedVersion2.GetBytes())
}

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_GetObject_NoKeyException() {
	// GIVEN
	key := ""

	// WHEN
	returnedVersion, err := s.persistentStorage.Get(key)

	// THEN
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrEmptyKey)
	s.Assert().Nil(returnedVersion)
}
