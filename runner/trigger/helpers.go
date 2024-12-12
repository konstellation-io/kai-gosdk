package trigger

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/konstellation-io/kai-gosdk/runner/common"
	"github.com/konstellation-io/kai-gosdk/sdk"
)

const (
	_initializerLoggerName     = "[INITIALIZER]"
	_runnerLoggerName          = "[RUNNER]"
	_responseHandlerLoggerName = "[RESPONSE HANDLER]"
	_finalizerLoggerName       = "[FINALIZER]"
)

func composeInitializer(initializer common.Initializer) common.Initializer {
	return func(sdk sdk.KaiSDK) {
		sdk.Logger.WithName(_initializerLoggerName).V(1).Info("Initializing TriggerRunner...")
		common.InitializeProcessConfiguration(sdk)

		if initializer != nil {
			sdk.Logger.WithName(_initializerLoggerName).V(3).Info("Executing user initializer...")
			initializer(sdk)
			sdk.Logger.WithName(_initializerLoggerName).V(3).Info("User initializer executed")
		}

		sdk.Logger.WithName(_initializerLoggerName).V(1).Info("TriggerRunner initialized")
	}
}

func composeRunner(userRunner RunnerFunc) RunnerFunc {
	return func(runner *Runner, kaiSDK sdk.KaiSDK) {
		kaiSDK.Logger.WithName(_runnerLoggerName).V(1).Info("Running TriggerRunner...")

		if userRunner != nil {
			kaiSDK.Logger.WithName(_runnerLoggerName).V(3).Info("Executing user runner...")

			go userRunner(runner, kaiSDK)
		}

		// Handle sigterm and await termChan signal
		termChan := make(chan os.Signal, 1)
		signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
		<-termChan

		kaiSDK.Logger.WithName(_runnerLoggerName).V(3).Info("User runner executed")

		// Handle shutdown
		kaiSDK.Logger.WithName(_runnerLoggerName).Info("Shutting down runner...")
		kaiSDK.Logger.WithName(_runnerLoggerName).V(1).Info("Closing opened channels...")
		runner.responseChannels.Range(func(key, value interface{}) bool {
			close(value.(chan *anypb.Any))
			kaiSDK.Logger.WithName(_runnerLoggerName).V(1).Info(fmt.Sprintf("Channel closed for request id %s", key))

			return true
		})

		kaiSDK.Logger.WithName(_runnerLoggerName).Info("RunnerFunc shutdown")
		wg.Done()
	}
}

func getResponseHandler(handlers *sync.Map) ResponseHandler {
	return func(kaiSDK sdk.KaiSDK, response *anypb.Any) error {
		// Unmarshal response to a KaiNatsMessage type
		kaiSDK.Logger.WithName(_responseHandlerLoggerName).
			Info(fmt.Sprintf("Message received with request id %s", kaiSDK.GetRequestID()))

		responseHandler, ok := handlers.LoadAndDelete(kaiSDK.GetRequestID())

		if ok {
			responseHandler.(chan *anypb.Any) <- response
			return nil
		}

		kaiSDK.Logger.WithName(_responseHandlerLoggerName).V(1).Info(fmt.Sprintf("Undefined handler for the message with request id %s",
			kaiSDK.GetRequestID()))

		return nil
	}
}

func composeFinalizer(userFinalizer common.Finalizer) common.Finalizer {
	return func(kaiSDK sdk.KaiSDK) {
		kaiSDK.Logger.WithName(_finalizerLoggerName).V(1).Info("Finalizing TriggerRunner...")

		if userFinalizer != nil {
			kaiSDK.Logger.WithName(_finalizerLoggerName).V(3).Info("Executing user finalizer...")
			userFinalizer(kaiSDK)
			kaiSDK.Logger.WithName(_finalizerLoggerName).V(3).Info("User finalizer executed")
		}

		kaiSDK.Logger.WithName(_finalizerLoggerName).V(1).Info("TriggerRunner finalized")
	}
}
