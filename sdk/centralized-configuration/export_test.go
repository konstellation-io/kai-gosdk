//go:build unit

package centralizedconfiguration

import (
	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
)

func NewBuilder(logger logr.Logger,
	globalKv, productKv, workflowKv, processKv nats.KeyValue) (*CentralizedConfiguration, error) {
	logger = logger.WithName("[CENTRALIZED CONFIGURATION]")

	return &CentralizedConfiguration{
		logger:     logger,
		globalKv:   globalKv,
		productKv:  productKv,
		workflowKv: workflowKv,
		processKv:  processKv,
	}, nil
}
