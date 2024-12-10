package runner

import (
	"fmt"
	"time"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/runner/exit"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/runner/task"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/runner/trigger"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type Runner struct {
	logger    logr.Logger
	nats      *nats.Conn
	jetstream nats.JetStreamContext
}

func NewRunner() *Runner {
	initializeConfiguration()

	logger := getLogger()

	nc, err := getNatsConnection(logger)
	if err != nil {
		panic(fmt.Errorf("fatal error connecting to NATS: %w", err))
	}

	js, err := getJetStreamConnection(logger, nc)
	if err != nil {
		panic(fmt.Errorf("fatal error connecting to JetStream: %w", err))
	}

	return &Runner{
		logger:    logger,
		nats:      nc,
		jetstream: js,
	}
}

func validateConfig(keys []string) {
	var mandatoryConfigKeys = []string{
		common.ConfigMetadataProductIDKey,
		common.ConfigMetadataVersionIDKey,
		common.ConfigMetadataWorkflowIDKey,
		common.ConfigMetadataWorkflowTypeKey,
		common.ConfigMetadataProcessIDKey,
		common.ConfigMetadataProcessTypeKey,
		common.ConfigNatsURLKey,
		common.ConfigNatsStreamKey,
		common.ConfigNatsOutputKey,
		common.ConfigCcGlobalBucketKey,
		common.ConfigCcProductBucketKey,
		common.ConfigCcWorkflowBucketKey,
		common.ConfigCcProcessBucketKey,
		common.ConfigMinioEndpointKey,
		common.ConfigMinioClientUserKey,
		common.ConfigMinioClientPasswordKey,
		common.ConfigMinioUseSslKey,
		common.ConfigMinioBucketKey,
		common.ConfigAuthEndpointKey,
		common.ConfigAuthClientKey,
		common.ConfigAuthClientSecretKey,
		common.ConfigAuthRealmKey,
		common.ConfigRedisUsernameKey,
		common.ConfigRedisPasswordKey,
		common.ConfigRedisEndpointKey,
		common.ConfigRedisIndexKey,
		common.ConfigMeasurementsEndpointKey,
		common.ConfigMeasurementsInsecureKey,
		common.ConfigMeasurementsTimeoutKey,
		common.ConfigMeasurementsMetricsIntervalKey,
	}

	for _, key := range mandatoryConfigKeys {
		if !slices.Contains(keys, key) {
			panic(fmt.Sprintf("missing mandatory configuration key: %s", key))
		}
	}
}

func initializeConfiguration() {
	// Load environment variables
	viper.SetEnvPrefix("KAI")
	viper.AutomaticEnv()

	if viper.IsSet(common.ConfigAppConfigPathKey) {
		viper.AddConfigPath(viper.GetString(common.ConfigAppConfigPathKey))
	}

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig() //nolint:ineffassign,staticcheck // err is used in the next if

	viper.SetConfigName("app")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if viper.IsSet(common.ConfigAppConfigPathKey) {
		viper.AddConfigPath(viper.GetString(common.ConfigAppConfigPathKey))
	}

	err = viper.MergeInConfig()

	keys := viper.AllKeys()
	if len(keys) == 0 {
		panic(fmt.Errorf("configuration could not be loaded: %w", err))
	}

	validateConfig(keys)

	// Set viper default values
	viper.SetDefault(common.ConfigRunnerSubscriberAckWaitTimeKey, 22*time.Hour)
	viper.SetDefault(common.ConfigRunnerLoggerLevelKey, "InfoLevel")
	viper.SetDefault(common.ConfigRunnerLoggerEncodingKey, "json")
	viper.SetDefault(common.ConfigRunnerLoggerOutputPathsKey, []string{"stdout"})
	viper.SetDefault(common.ConfigRunnerLoggerErrorOutputPathsKey, []string{"stderr"})
	viper.SetDefault(common.ConfigMinioInternalFolderKey, ".kai")
	viper.SetDefault(common.ConfigModelFolderNameKey, ".models")
}

func getNatsConnection(logger logr.Logger) (*nats.Conn, error) {
	nc, err := nats.Connect(viper.GetString(common.ConfigNatsURLKey))
	if err != nil {
		logger.Error(err, "Error connecting to NATS")

		return nil, err
	}

	return nc, nil
}

func getJetStreamConnection(logger logr.Logger, nc *nats.Conn) (nats.JetStreamContext, error) {
	js, err := nc.JetStream()
	if err != nil {
		logger.Error(err, "Error connecting to JetStream")
		return nil, err
	}

	return js, nil
}

func getLogger() logr.Logger {
	var log logr.Logger

	config := zap.NewProductionConfig()

	logLevel, err := zap.ParseAtomicLevel(viper.GetString(common.ConfigRunnerLoggerLevelKey))
	if err != nil {
		logLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	config.Level = zap.NewAtomicLevelAt(logLevel.Level())
	config.OutputPaths = viper.GetStringSlice(common.ConfigRunnerLoggerOutputPathsKey)
	config.ErrorOutputPaths = viper.GetStringSlice(common.ConfigRunnerLoggerErrorOutputPathsKey)
	config.Encoding = viper.GetString(common.ConfigRunnerLoggerEncodingKey)

	logger, err := config.Build()
	if err != nil {
		panic("The logger could not be initialized")
	}

	defer logger.Sync() //nolint:errcheck // Ignore error

	log = zapr.NewLogger(logger)

	log.WithName("[RUNNER CONFIG]").V(1).Info(fmt.Sprintf("Logger initialized with level %s", logLevel.String()))

	return log
}

func (rn Runner) TriggerRunner() *trigger.Runner {
	return trigger.NewTriggerRunner(rn.logger, rn.nats, rn.jetstream)
}

func (rn Runner) TaskRunner() *task.Runner {
	return task.NewTaskRunner(rn.logger, rn.nats, rn.jetstream)
}

func (rn Runner) ExitRunner() *exit.Runner {
	return exit.NewExitRunner(rn.logger, rn.nats, rn.jetstream)
}
