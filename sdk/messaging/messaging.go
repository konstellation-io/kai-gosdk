package messaging

import (
	"github.com/go-logr/logr"
	kai "github.com/konstellation-io/kai-gosdk/protos"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const _messagingLoggerName = "[MESSAGING]"

type Messaging struct {
	logger         logr.Logger
	nats           *nats.Conn
	jetstream      nats.JetStreamContext
	requestMessage *kai.KaiNatsMessage
	messagingUtils messagingUtils
}

func New(logger logr.Logger, ns *nats.Conn, js nats.JetStreamContext,
	requestMessage *kai.KaiNatsMessage,
) *Messaging {
	return &Messaging{
		logger,
		ns,
		js,
		requestMessage,
		NewMessagingUtils(ns, js),
	}
}

func (ms Messaging) SendOutput(response proto.Message, channelOpt ...string) error {
	return ms.publishMsg(response, ms.requestMessage.GetRequestId(), kai.MessageType_OK, ms.getOptionalString(channelOpt))
}

func (ms Messaging) SendOutputWithRequestID(response proto.Message, requestID string, channelOpt ...string) error {
	return ms.publishMsg(response, requestID, kai.MessageType_OK, ms.getOptionalString(channelOpt))
}

func (ms Messaging) SendAny(response *anypb.Any, channelOpt ...string) {
	ms.publishAny(response, ms.requestMessage.GetRequestId(), kai.MessageType_OK, ms.getOptionalString(channelOpt))
}

func (ms Messaging) SendAnyWithRequestID(response *anypb.Any, requestID string, channelOpt ...string) {
	ms.publishAny(response, requestID, kai.MessageType_OK, ms.getOptionalString(channelOpt))
}

func (ms Messaging) SendError(errorMessage string, channelOpt ...string) {
	ms.publishError(ms.requestMessage.GetRequestId(), errorMessage, ms.getOptionalString(channelOpt))
}

func (ms Messaging) GetErrorMessage() string {
	if ms.IsMessageError() {
		return ms.requestMessage.GetError()
	}

	return ""
}

func (ms Messaging) IsMessageOK() bool {
	return ms.requestMessage.GetMessageType() == kai.MessageType_OK
}

func (ms Messaging) IsMessageError() bool {
	return ms.requestMessage.GetMessageType() == kai.MessageType_ERROR
}
