package exit

import (
	"strings"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/runner/common"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk"
	"go.opentelemetry.io/otel/metric"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
)

const _exitLoggerName = "[EXIT]"

type Preprocessor common.Handler

type Handler common.Handler

type Postprocessor common.Handler

type Runner struct {
	sdk              sdk.KaiSDK
	nats             *nats.Conn
	jetstream        nats.JetStreamContext
	responseHandlers map[string]Handler
	initializer      common.Initializer
	preprocessor     Preprocessor
	postprocessor    Postprocessor
	finalizer        common.Finalizer
	messagesMetric   metric.Int64Histogram
}

func NewExitRunner(logger logr.Logger, ns *nats.Conn, js nats.JetStreamContext) *Runner {
	return &Runner{
		sdk:              sdk.NewKaiSDK(logger.WithName(_exitLoggerName), ns, js),
		nats:             ns,
		jetstream:        js,
		responseHandlers: make(map[string]Handler),
	}
}

func (er *Runner) WithInitializer(initializer common.Initializer) *Runner {
	er.initializer = composeInitializer(initializer)
	return er
}

func (er *Runner) WithPreprocessor(preprocessor Preprocessor) *Runner {
	er.preprocessor = composePreprocessor(preprocessor)
	return er
}

func (er *Runner) WithHandler(handler Handler) *Runner {
	er.responseHandlers["default"] = composeHandler(handler)
	return er
}

func (er *Runner) WithCustomHandler(subject string, handler Handler) *Runner {
	er.responseHandlers[strings.ToLower(subject)] = composeHandler(handler)
	return er
}

func (er *Runner) WithPostprocessor(postprocessor Postprocessor) *Runner {
	er.postprocessor = composePostprocessor(postprocessor)
	return er
}

func (er *Runner) WithFinalizer(finalizer common.Finalizer) *Runner {
	er.finalizer = composeFinalizer(finalizer)
	return er
}

func (er *Runner) Run() {
	if er.responseHandlers["default"] == nil {
		panic("Undefined default handler")
	}

	if er.initializer == nil {
		er.initializer = composeInitializer(nil)
	}

	if er.finalizer == nil {
		er.finalizer = composeFinalizer(nil)
	}

	er.initializer(er.sdk)

	er.startSubscriber()

	er.finalizer(er.sdk)
}
