package messaging

import (
	"fmt"

	"github.com/konstellation-io/kai-gosdk/internal/common"

	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
)

//go:generate mockery --name messagingUtils --output ../../mocks --structname MessagingUtilsMock --filename messaging_utils_mock.go
type messagingUtils interface {
	GetMaxMessageSize() (int64, error)
}

type MessagingUtilsImpl struct { //nolint:revive // naming is correct
	jetstream nats.JetStreamContext
	nats      *nats.Conn
}

func NewMessagingUtils(ns *nats.Conn, js nats.JetStreamContext) MessagingUtilsImpl {
	return MessagingUtilsImpl{
		nats:      ns,
		jetstream: js,
	}
}

func (mu MessagingUtilsImpl) GetMaxMessageSize() (int64, error) {
	streamInfo, err := mu.jetstream.StreamInfo(viper.GetString(common.ConfigNatsStreamKey))
	if err != nil {
		return 0, fmt.Errorf("error getting stream's max message size: %w", err)
	}

	streamMaxSize := int64(streamInfo.Config.MaxMsgSize)
	serverMaxSize := mu.nats.MaxPayload()

	if streamMaxSize == -1 {
		return serverMaxSize, nil
	}

	if streamMaxSize < serverMaxSize {
		return streamMaxSize, nil
	}

	return serverMaxSize, nil
}

func sizeInMB(size int64) string {
	mbSize := float32(size) / 1024 / 1024
	return fmt.Sprintf("%.1f MB", mbSize)
}
