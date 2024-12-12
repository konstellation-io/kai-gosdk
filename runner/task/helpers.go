package task

import (
	"github.com/konstellation-io/kai-gosdk/runner/common"
	"github.com/konstellation-io/kai-gosdk/sdk"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	_initializerLoggerName   = "[INITIALIZER]"
	_preprocessorLoggerName  = "[PREPROCESSOR]"
	_handlerLoggerName       = "[HANDLER]"
	_postprocessorLoggerName = "[POSTPROCESSOR]"
	_finalizerLoggerName     = "[FINALIZER]"
)

func composeInitializer(initializer common.Initializer) common.Initializer {
	return func(kaiSDK sdk.KaiSDK) {
		kaiSDK.Logger.WithName(_initializerLoggerName).V(1).Info("Initializing TaskRunner...")
		common.InitializeProcessConfiguration(kaiSDK)

		if initializer != nil {
			kaiSDK.Logger.WithName(_initializerLoggerName).V(3).Info("Executing user initializer...")
			initializer(kaiSDK)
			kaiSDK.Logger.WithName(_initializerLoggerName).V(3).Info("User initializer executed")
		}

		kaiSDK.Logger.WithName(_initializerLoggerName).V(1).Info("TaskRunner initialized")
	}
}

func composePreprocessor(preprocessor Preprocessor) Preprocessor {
	return func(kaiSDK sdk.KaiSDK, response *anypb.Any) error {
		kaiSDK.Logger.WithName(_preprocessorLoggerName).V(1).Info("Preprocessing TaskRunner...")

		if preprocessor != nil {
			kaiSDK.Logger.WithName(_preprocessorLoggerName).V(3).Info("Executing user preprocessor...")
			return preprocessor(kaiSDK, response)
		}

		return nil
	}
}

func composeHandler(handler Handler) Handler {
	return func(kaiSDK sdk.KaiSDK, response *anypb.Any) error {
		kaiSDK.Logger.WithName(_handlerLoggerName).V(1).Info("Handling TaskRunner...")

		if handler != nil {
			kaiSDK.Logger.WithName(_handlerLoggerName).V(3).Info("Executing user handler...")
			return handler(kaiSDK, response)
		}

		return nil
	}
}

func composePostprocessor(postprocessor Postprocessor) Postprocessor {
	return func(kaiSDK sdk.KaiSDK, response *anypb.Any) error {
		kaiSDK.Logger.WithName(_postprocessorLoggerName).V(1).Info("Postprocessing TaskRunner...")

		if postprocessor != nil {
			kaiSDK.Logger.WithName(_postprocessorLoggerName).V(3).Info("Executing user postprocessor...")
			return postprocessor(kaiSDK, response)
		}

		return nil
	}
}

func composeFinalizer(finalizer common.Finalizer) common.Finalizer {
	return func(kaiSDK sdk.KaiSDK) {
		kaiSDK.Logger.WithName(_finalizerLoggerName).V(1).Info("Finalizing TaskRunner...")

		if finalizer != nil {
			kaiSDK.Logger.WithName(_finalizerLoggerName).V(3).Info("Executing user finalizer...")
			finalizer(kaiSDK)
			kaiSDK.Logger.WithName(_finalizerLoggerName).V(3).Info("User finalizer executed")
		}
	}
}
