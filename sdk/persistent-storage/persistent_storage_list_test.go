//go:build integration

package persistentstorage_test

import (
	"bytes"
	"context"

	"github.com/minio/minio-go/v7"
)

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_ListObject_ExpectOK() {
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
	listObjects, err := s.persistentStorage.List()

	// THEN
	s.Assert().NoError(err)
	s.Assert().Len(listObjects, 1)
	s.Assert().Equal(obj.Key, listObjects[0].Key)
	s.Assert().Equal(obj.VersionID, listObjects[0].VersionID)
}

func (s *SdkPersistentStorageTestSuite) TestPersistentStorage_ListObject_EmptyList_ExpectOK() {
	// WHEN
	returnedVersion, err := s.persistentStorage.List()

	// THEN
	s.Assert().NoError(err)
	s.Assert().Empty(returnedVersion)
}
