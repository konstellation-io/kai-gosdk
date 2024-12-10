//go:build integration

package persistentstorage_test

import (
	"bytes"
	"context"

	persistentstorage "github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/persistent-storage"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"
	"github.com/minio/minio-go/v7"
)

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_ListObjectVersions_ExpectOK() {
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
	objectVersions, err := s.persistentStorage.ListVersions(key)

	// THEN
	s.Assert().NoError(err)
	s.Assert().Len(objectVersions, 2)
	s.Assert().Equal(objectVersions, []*persistentstorage.ObjectInfo{
		{Key: key, VersionID: obj2.VersionID},
		{Key: key, VersionID: obj.VersionID},
	})
}

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_ListObjectVersions_EmptyList_ExpectOK() {
	// WHEN
	returnedVersion, err := s.persistentStorage.ListVersions("some-key")

	// THEN
	s.Assert().NoError(err)
	s.Assert().Len(returnedVersion, 0)
	s.Assert().Empty(returnedVersion)
}

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_ListObjectVersions_NoKey_ExpectError() {
	// WHEN
	returnedVersion, err := s.persistentStorage.ListVersions("")

	// THEN
	s.Assert().Error(err)
	s.Assert().ErrorIs(err, errors.ErrEmptyKey)
	s.Assert().Nil(returnedVersion)
}
