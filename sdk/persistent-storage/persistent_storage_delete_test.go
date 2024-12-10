//go:build integration

package persistentstorage_test

import (
	"bytes"
	"context"

	"github.com/minio/minio-go/v7"
)

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_DeleteObject_ExpectOK() {
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
	err = s.persistentStorage.Delete(key)

	// THEN
	s.Assert().NoError(err)
	info, err := s.client.StatObject(context.Background(), s.persistentStorageBucket, key, minio.StatObjectOptions{})
	s.Assert().Error(err)
	s.Assert().Empty(info)
}

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_DeleteObjectVersion_ExpectOK() {
	// GIVEN
	key := "some-object"
	data := []byte("some-data")
	obj, err := s.client.PutObject(
		context.Background(),
		s.persistentStorageBucket,
		key,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{},
	)
	s.Require().NoError(err)

	// WHEN
	err = s.persistentStorage.Delete(key, obj.VersionID)

	// THEN
	s.Assert().NoError(err)
	info, err := s.client.StatObject(context.Background(), s.persistentStorageBucket, key, minio.StatObjectOptions{})
	s.Assert().Error(err)
	s.Assert().Empty(info)
}
