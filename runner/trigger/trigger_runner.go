package trigger

import (
	"sync"

	"github.com/go-logr/logr"
	"github.com/konstellation-io/kai-gosdk/runner/common"
	"github.com/konstellation-io/kai-gosdk/sdk"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/types/known/anypb"
)

const _triggerLoggerName = "[TRIGGER]"

type RunnerFunc func(tr *Runner, sdk sdk.KaiSDK)

type ResponseHandler func(sdk sdk.KaiSDK, response *anypb.Any) error

type Runner struct {
	sdk              sdk.KaiSDK
	nats             *nats.Conn
	jetstream        nats.JetStreamContext
	responseHandler  ResponseHandler
	responseChannels sync.Map
	initializer      common.Initializer
	runner           RunnerFunc
	finalizer        common.Finalizer
	messagesMetric   metric.Int64Histogram
}

var wg sync.WaitGroup //nolint:gochecknoglobals // WaitGroup is used to wait for goroutines to finish

func NewTriggerRunner(logger logr.Logger, ns *nats.Conn, js nats.JetStreamContext) *Runner {
	return &Runner{
		sdk:              sdk.NewKaiSDK(logger.WithName(_triggerLoggerName), ns, js),
		nats:             ns,
		jetstream:        js,
		responseChannels: sync.Map{},
	}
}

func (tr *Runner) WithInitializer(initializer common.Initializer) *Runner {
	tr.initializer = composeInitializer(initializer)
	return tr
}

func (tr *Runner) WithRunner(runner RunnerFunc) *Runner {
	tr.runner = composeRunner(runner)
	return tr
}

func (tr *Runner) WithFinalizer(finalizer common.Finalizer) *Runner {
	tr.finalizer = composeFinalizer(finalizer)
	return tr
}

func (tr *Runner) GetResponseChannel(requestID string) <-chan *anypb.Any {
	tr.responseChannels.Store(requestID, make(chan *anypb.Any))
	channel, _ := tr.responseChannels.Load(requestID)

	return channel.(chan *anypb.Any)
}

func (tr *Runner) Run() {
	// Check required fields are initialized
	if tr.runner == nil {
		panic("Undefined runner function")
	}

	if tr.initializer == nil {
		tr.initializer = composeInitializer(nil)
	}

	tr.responseHandler = getResponseHandler(&tr.responseChannels)

	if tr.finalizer == nil {
		tr.finalizer = composeFinalizer(nil)
	}

	tr.initializer(tr.sdk)

	delta := 2
	wg.Add(delta)

	go tr.runner(tr, tr.sdk)

	go tr.startSubscriber()

	wg.Wait()

	tr.finalizer(tr.sdk)
}
