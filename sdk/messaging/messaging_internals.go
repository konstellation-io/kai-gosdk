package messaging

import (
	"fmt"

	"github.com/konstellation-io/kai-gosdk/internal/errors"
	"github.com/nats-io/nats.go"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/konstellation-io/kai-gosdk/internal/common"
	kai "github.com/konstellation-io/kai-gosdk/protos"
)

const (
	defaultValue = ""
)

func (ms Messaging) getOptionalString(values []string) string {
	if len(values) > 0 {
		return values[0]
	}

	return defaultValue
}

func (ms Messaging) publishMsg(msg proto.Message, requestID string, msgType kai.MessageType, channel string) error {
	payload, err := anypb.New(msg)
	if err != nil {
		return fmt.Errorf("the handler result is not a valid protobuf: %w", err)
	}

	if requestID == "" {
		requestID = uuid.New().String()
	}

	responseMsg := ms.newResponseMsg(payload, requestID, msgType)

	ms.publishResponse(responseMsg, channel)

	return nil
}

func (ms Messaging) publishAny(payload *anypb.Any, requestID string, msgType kai.MessageType, channel string) {
	if requestID == "" {
		requestID = uuid.New().String()
	}

	responseMsg := ms.newResponseMsg(payload, requestID, msgType)
	ms.publishResponse(responseMsg, channel)
}

func (ms Messaging) publishError(requestID, errMsg, channel string) {
	responseMsg := &kai.KaiNatsMessage{
		RequestId:   requestID,
		Error:       errMsg,
		FromNode:    viper.GetString(common.ConfigMetadataProcessIDKey),
		MessageType: kai.MessageType_ERROR,
	}
	ms.publishResponse(responseMsg, channel)
}

func (ms Messaging) newResponseMsg(payload *anypb.Any, requestID string,
	msgType kai.MessageType,
) *kai.KaiNatsMessage {
	ms.logger.WithName(_messagingLoggerName).V(1).Info(fmt.Sprintf("Preparing response message for "+
		"request id %s and message type %s", requestID, msgType))

	return &kai.KaiNatsMessage{
		RequestId:   requestID,
		Payload:     payload,
		FromNode:    viper.GetString(common.ConfigMetadataProcessIDKey),
		MessageType: msgType,
	}
}

func (ms Messaging) publishResponse(responseMsg *kai.KaiNatsMessage, channel string) {
	outputSubject := ms.getOutputSubject(channel)

	outputMsg, err := proto.Marshal(responseMsg)
	if err != nil {
		ms.logger.WithName(_messagingLoggerName).
			Error(err, fmt.Sprintf("Error generating output result because "+
				"handler result is not a serializable Protobuf for request id %s", responseMsg.GetRequestId()))

		return
	}

	outputMsg, err = ms.prepareOutputMessage(outputMsg)
	if err != nil {
		ms.logger.WithName(_messagingLoggerName).
			Error(err, fmt.Sprintf("Error preparing output message for request id %s", responseMsg.GetRequestId()))

		return
	}

	ms.logger.WithName(_messagingLoggerName).Info(fmt.Sprintf("Publishing response with subject %s "+
		"for request id %s", outputSubject, responseMsg.GetRequestId()))

	_, err = ms.jetstream.Publish(outputSubject, outputMsg)
	if err != nil {
		ms.logger.WithName(_messagingLoggerName).
			Error(err, fmt.Sprintf("Error publishing output for"+
				" request id %s", responseMsg.GetRequestId()))
	}
}

func (ms Messaging) getOutputSubject(channel string) string {
	outputSubject := viper.GetString(common.ConfigNatsOutputKey)
	if channel != "" {
		return fmt.Sprintf("%s.%s", outputSubject, channel)
	}

	return outputSubject
}

func (ms Messaging) prepareOutputMessage(msg []byte) ([]byte, error) {
	maxSize, err := ms.messagingUtils.GetMaxMessageSize()
	if err != nil {
		return nil, fmt.Errorf("error getting max message size: %w", err)
	}

	lenMsg := int64(len(msg))
	if lenMsg <= maxSize {
		return msg, nil
	}

	ms.logger.WithName(_messagingLoggerName).V(1).
		Info("Message exceeds maximum size allowed, compressing data")

	outMsg, err := common.CompressData(msg)
	if err != nil {
		return nil, err
	}

	lenOutMsg := int64(len(outMsg))
	if lenOutMsg > maxSize {
		ms.logger.WithName(_messagingLoggerName).V(1).
			Info("Compressed message size %s exceeds maximum size allowed %s",
				sizeInMB(lenOutMsg),
				sizeInMB(maxSize),
			)

		return nil, errors.ErrMessageToBig
	}

	ms.logger.WithName(_messagingLoggerName).
		Info(fmt.Sprintf("Message prepared with original size %s and compressed size %s", sizeInMB(lenMsg),
			sizeInMB(lenOutMsg)))

	return outMsg, nil
}

func (ms Messaging) GetRequestID(msg *nats.Msg) (string, error) {
	requestMsg := &kai.KaiNatsMessage{}

	data := msg.Data

	var err error
	if common.IsCompressed(data) {
		data, err = common.UncompressData(data)
		if err != nil {
			ms.logger.WithName(_messagingLoggerName).Error(err, "Error reading compressed message")
			return "", err
		}
	}

	err = proto.Unmarshal(data, requestMsg)
	if err != nil {
		ms.logger.WithName(_messagingLoggerName).Error(err, "Error unmarshalling message")
		return "", err
	}

	return requestMsg.GetRequestId(), err
}
