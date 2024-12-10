package measurement

import (
	"fmt"
	"time"

	"context"

	"github.com/go-logr/logr"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk/metadata"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkMetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

const (
	_persistentStorageLoggerName = "[MEASUREMENTS]"
)

type Measurement struct {
	logger        logr.Logger
	metricsClient metric.Meter
	metadata      *metadata.Metadata
}

func New(logger logr.Logger, meta *metadata.Metadata) (*Measurement, error) {
	endpoint := viper.GetString(common.ConfigMeasurementsEndpointKey)
	insecure := viper.GetBool(common.ConfigMeasurementsInsecureKey)
	timeout := viper.GetInt(common.ConfigMeasurementsTimeoutKey)
	interval := viper.GetInt(common.ConfigMeasurementsMetricsIntervalKey)

	metricsClient, err := initMetrics(logger, endpoint, insecure, timeout, interval, meta)
	if err != nil {
		return nil, err
	}

	return &Measurement{
		logger:        logger,
		metricsClient: metricsClient,
		metadata:      meta,
	}, nil
}

func (m Measurement) GetMetricsClient() metric.Meter {
	return m.metricsClient
}

func initMetrics(logger logr.Logger, endpoint string, insecure bool, timeout, interval int, meta *metadata.Metadata) (metric.Meter, error) {
	res, err := initResource(meta)
	if err != nil {
		return nil, fmt.Errorf("error initializing metrics: %w", err)
	}

	exporter, err := initExporter(endpoint, insecure, timeout)
	if err != nil {
		return nil, fmt.Errorf("error initializing metrics: %w", err)
	}

	provider := initProvider(exporter, res, interval)

	otel.SetMeterProvider(provider)

	logger.WithName(_persistentStorageLoggerName).Info("Successfully initialized metrics")

	return provider.Meter("measurements"), nil
}

func initResource(meta *metadata.Metadata) (*resource.Resource, error) {
	return resource.Merge(resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(meta.GetProduct()),
			semconv.ServiceVersion(meta.GetVersion()),
			semconv.ServiceNamespace(meta.GetWorkflow()),
			semconv.ServiceInstanceID(meta.GetProcess()),
		),
	)
}

func initExporter(endpoint string, insecure bool, timeout int) (*otlpmetricgrpc.Exporter, error) {
	if insecure {
		return otlpmetricgrpc.New(
			context.Background(),
			otlpmetricgrpc.WithEndpoint(endpoint),
			otlpmetricgrpc.WithTimeout(time.Duration(timeout)*time.Second),
			otlpmetricgrpc.WithInsecure(),
		)
	}

	return otlpmetricgrpc.New(
		context.Background(),
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithTimeout(time.Duration(timeout)*time.Second),
	)
}

func initProvider(exporter *otlpmetricgrpc.Exporter, res *resource.Resource, interval int) *sdkMetric.MeterProvider {
	return sdkMetric.NewMeterProvider(
		sdkMetric.WithResource(res),
		sdkMetric.WithReader(
			sdkMetric.NewPeriodicReader(
				exporter,
				sdkMetric.WithInterval(time.Duration(interval)*time.Second),
			),
		),
	)
}
