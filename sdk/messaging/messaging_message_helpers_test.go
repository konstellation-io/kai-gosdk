//go:build unit

package messaging_test

import (
	kai "github.com/konstellation-io/kai-sdk/go-sdk/v1/protos"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/messaging"
)

const (
	requestIDValue = "some-request-id"
	errorMessage   = "Some error message"
)

func (s *SdkMessagingTestSuite) TestMessaging_GetErrorMessage_ExpectOk() {
	// Given
	kaiMessage := &kai.KaiNatsMessage{
		RequestId:   requestIDValue,
		MessageType: kai.MessageType_ERROR,
		Error:       "Error message",
	}
	objectStore := messaging.NewTestMessaging(s.logger, nil, &s.jetstream, kaiMessage, &s.messagingUtils)

	// When
	errorMessage := objectStore.GetErrorMessage()

	// Then
	s.NotNil(objectStore)
	s.Equal("Error message", errorMessage)
}

func (s *SdkMessagingTestSuite) TestMessaging_GetErrorMessage_NoErrorMessageExistWhenTypeOK_ExpectError() {
	// Given
	kaiMessage := &kai.KaiNatsMessage{
		RequestId:   requestIDValue,
		MessageType: kai.MessageType_OK,
		Error:       errorMessage,
	}
	objectStore := messaging.NewTestMessaging(s.logger, nil, &s.jetstream, kaiMessage, &s.messagingUtils)

	// When
	errorMessage := objectStore.GetErrorMessage()

	// Then
	s.NotNil(objectStore)
	s.Empty(errorMessage)
}

func (s *SdkMessagingTestSuite) TestMessaging_IsMessageOk_MessageOk_ExpectTrue() {
	// Given
	kaiMessage := &kai.KaiNatsMessage{
		RequestId:   requestIDValue,
		MessageType: kai.MessageType_OK,
	}
	objectStore := messaging.NewTestMessaging(s.logger, nil, &s.jetstream, kaiMessage, &s.messagingUtils)

	// When
	ok := objectStore.IsMessageOK()

	// Then
	s.NotNil(objectStore)
	s.True(ok)
}

func (s *SdkMessagingTestSuite) TestMessaging_IsMessageOk_MessageNotOk_ExpectFalse() {
	// Given
	kaiMessage := &kai.KaiNatsMessage{
		RequestId:   requestIDValue,
		MessageType: kai.MessageType_ERROR,
		Error:       errorMessage,
	}
	objectStore := messaging.NewTestMessaging(s.logger, nil, &s.jetstream, kaiMessage, &s.messagingUtils)

	// When
	ok := objectStore.IsMessageOK()

	// Then
	s.NotNil(objectStore)
	s.False(ok)
}

func (s *SdkMessagingTestSuite) TestMessaging_IsMessageError_MessageError_ExpectTrue() {
	// Given
	kaiMessage := &kai.KaiNatsMessage{
		RequestId:   requestIDValue,
		MessageType: kai.MessageType_ERROR,
		Error:       errorMessage,
	}
	objectStore := messaging.NewTestMessaging(s.logger, nil, &s.jetstream, kaiMessage, &s.messagingUtils)

	// When
	isError := objectStore.IsMessageError()

	// Then
	s.NotNil(objectStore)
	s.True(isError)
}

func (s *SdkMessagingTestSuite) TestMessaging_IsMessageError_MessageNotError_ExpectFalse() {
	// Given
	kaiMessage := &kai.KaiNatsMessage{
		RequestId:   requestIDValue,
		MessageType: kai.MessageType_OK,
	}
	objectStore := messaging.NewTestMessaging(s.logger, nil, &s.jetstream, kaiMessage, &s.messagingUtils)

	// When
	isError := objectStore.IsMessageError()

	// Then
	s.NotNil(objectStore)
	s.False(isError)
}
