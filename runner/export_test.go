//go:build unit

package runner

import (
	"github.com/nats-io/nats.go"
)

func NewTestRunner(ns *nats.Conn, js nats.JetStreamContext) *Runner {
	initializeConfiguration()

	return &Runner{
		logger:    getLogger(),
		nats:      ns,
		jetstream: js,
	}
}
