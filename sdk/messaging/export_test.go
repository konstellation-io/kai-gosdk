//go:build unit

package messaging

import (
	"github.com/go-logr/logr"
	kai "github.com/konstellation-io/kai-gosdk/protos"
	"github.com/nats-io/nats.go"
)

func NewTestMessaging(logger logr.Logger, ns *nats.Conn, js nats.JetStreamContext,
	requestMessage *kai.KaiNatsMessage, messagingUtils messagingUtils,
) *Messaging {
	return &Messaging{
		logger.WithName("[MESSAGING]"),
		ns,
		js,
		requestMessage,
		messagingUtils,
	}
}
