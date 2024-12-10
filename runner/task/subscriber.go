package task

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/errors"
	kai "github.com/konstellation-io/kai-sdk/go-sdk/v1/protos"
	"github.com/konstellation-io/kai-sdk/go-sdk/v1/sdk"
)

const _subscriberLoggerName = "[SUBSCRIBER]"

func (tr *Runner) getLoggerWithName() logr.Logger {
	return tr.sdk.Logger.WithName(_subscriberLoggerName)
}

func (tr *Runner) startSubscriber() {
	inputSubjects := viper.GetStringSlice(common.ConfigNatsInputsKey)

	if len(inputSubjects) == 0 {
		tr.getLoggerWithName().Info("Undefined input subjects")
		os.Exit(1)
	}

	var err error

	tr.messagesMetric, err = tr.sdk.Measurements.GetMetricsClient().Int64Histogram(
		"runner-process-message-metric",
		metric.WithDescription("How long it takes to process a message and times called."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		tr.getLoggerWithName().Error(err, "Error initializing metric")
		os.Exit(1)
	}

	subscriptions := make([]*nats.Subscription, 0, len(inputSubjects))

	for _, subject := range inputSubjects {
		consumerName := fmt.Sprintf("%s-%s", strings.ReplaceAll(subject, ".", "-"),
			strings.ReplaceAll(strings.ReplaceAll(tr.sdk.Metadata.GetProcess(), ".", "-"), " ", "-"))

		tr.getLoggerWithName().V(1).Info(fmt.Sprintf("Subscribing to subject %s with queue group %s", subject, consumerName))

		s, err := tr.jetstream.QueueSubscribe(
			subject,
			consumerName,
			tr.processMessage,
			nats.DeliverNew(),
			nats.Durable(consumerName),
			nats.ManualAck(),
			nats.AckWait(viper.GetDuration(common.ConfigRunnerSubscriberAckWaitTimeKey)),
		)
		if err != nil {
			tr.getLoggerWithName().Error(err, fmt.Sprintf("Error subscribing to subject %s", subject))
			os.Exit(1)
		}

		subscriptions = append(subscriptions, s)

		tr.getLoggerWithName().V(1).Info(fmt.Sprintf("Listening to subject %s with queue group %s", subject, consumerName))
	}

	tr.getLoggerWithName().V(1).Info("Subscribed to all subjects successfully")

	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan

	// Handle shutdown
	tr.getLoggerWithName().Info("Shutdown signal received")

	tr.getLoggerWithName().V(1).Info("Unsubscribing from all subjects")

	for _, s := range subscriptions {
		tr.getLoggerWithName().V(1).Info(fmt.Sprintf("Unsubscribing from subject %s", s.Subject))

		err := s.Unsubscribe()
		if err != nil {
			tr.getLoggerWithName().Error(err, fmt.Sprintf("Error unsubscribing from the subject %s", s.Subject))
			os.Exit(1)
		}
	}

	tr.getLoggerWithName().Info("Unsubscribed from all subjects")
}

func (tr *Runner) processMessage(msg *nats.Msg) {
	requestMsg, err := tr.newRequestMessage(msg.Data)
	if err != nil {
		errMsg := fmt.Sprintf("Error parsing msg.data coming from subject %s because is not a valid protobuf: %s", msg.Subject, err)
		tr.processRunnerError(msg, errMsg, requestMsg.RequestId)

		return
	}

	start := time.Now()
	defer func() {
		executionTime := time.Since(start).Milliseconds()
		tr.sdk.Logger.V(1).Info(fmt.Sprintf("%s execution time: %d ms", tr.sdk.Metadata.GetProcess(), executionTime))

		tr.messagesMetric.Record(context.Background(), executionTime,
			metric.WithAttributeSet(tr.getMetricAttributes(requestMsg.RequestId)),
		)
	}()

	tr.getLoggerWithName().Info(fmt.Sprintf("New message received with subject %s",
		msg.Subject))

	handler := tr.getResponseHandler(strings.ToLower(requestMsg.FromNode))
	if handler == nil {
		errMsg := fmt.Sprintf("Error missing handler for node %q", requestMsg.FromNode)
		tr.processRunnerError(msg, errMsg, requestMsg.RequestId)

		return
	}

	// Make a shallow copy of the sdk object to set inside the request msg.
	hSdk := sdk.ShallowCopyWithRequest(&tr.sdk, requestMsg)

	if tr.preprocessor != nil {
		err := tr.preprocessor(hSdk, requestMsg.Payload)
		if err != nil {
			errMsg := fmt.Sprintf("Error in node %q executing handler preprocessor for node %q: %s",
				tr.sdk.Metadata.GetProcess(), requestMsg.FromNode, err)
			tr.processRunnerError(msg, errMsg, requestMsg.RequestId)

			return
		}
	}

	err = handler(hSdk, requestMsg.Payload)
	if err != nil {
		errMsg := fmt.Sprintf("Error in node %q executing handler for node %q: %s",
			tr.sdk.Metadata.GetProcess(), requestMsg.FromNode, err)
		tr.processRunnerError(msg, errMsg, requestMsg.RequestId)

		return
	}

	if tr.postprocessor != nil {
		err := tr.postprocessor(hSdk, requestMsg.Payload)
		if err != nil {
			errMsg := fmt.Sprintf("Error in node %q executing handler postprocessor for node %q: %s",
				tr.sdk.Metadata.GetProcess(), requestMsg.FromNode, err)
			tr.processRunnerError(msg, errMsg, requestMsg.RequestId)

			return
		}
	}

	// Tell NATS we don't need to receive the message anymore, and we are done processing it.
	ackErr := msg.Ack()
	if ackErr != nil {
		tr.getLoggerWithName().Error(ackErr, errors.ErrMsgAck)
	}
}

func (tr *Runner) processRunnerError(msg *nats.Msg, errMsg, requestID string) {
	ackErr := msg.Ack()
	if ackErr != nil {
		tr.getLoggerWithName().Error(ackErr, errors.ErrMsgAck)
	}

	tr.getLoggerWithName().V(1).Info(errMsg)
	tr.publishError(requestID, errMsg)
}

func (tr *Runner) newRequestMessage(data []byte) (*kai.KaiNatsMessage, error) {
	requestMsg := &kai.KaiNatsMessage{}

	var err error
	if common.IsCompressed(data) {
		data, err = common.UncompressData(data)
		if err != nil {
			tr.getLoggerWithName().Error(err, "Error reading compressed message")
			return nil, err
		}
	}

	err = proto.Unmarshal(data, requestMsg)

	return requestMsg, err
}

func (tr *Runner) publishError(requestID, errMsg string) {
	responseMsg := &kai.KaiNatsMessage{
		RequestId:   requestID,
		Error:       errMsg,
		FromNode:    viper.GetString(common.ConfigMetadataProcessIDKey),
		MessageType: kai.MessageType_ERROR,
	}
	tr.publishResponse(responseMsg, "")
}

func (tr *Runner) publishResponse(responseMsg *kai.KaiNatsMessage, channel string) {
	outputSubject := tr.getOutputSubject(channel)

	outputMsg, err := proto.Marshal(responseMsg)
	if err != nil {
		tr.getLoggerWithName().
			Error(err, "Error generating output result because handler result is not a serializable Protobuf")
		return
	}

	outputMsg, err = tr.prepareOutputMessage(outputMsg)
	if err != nil {
		tr.getLoggerWithName().Error(err, "Error preparing output message")
		return
	}

	tr.getLoggerWithName().V(1).Info(fmt.Sprintf("Publishing response with subject %s", outputSubject))

	_, err = tr.jetstream.Publish(outputSubject, outputMsg)
	if err != nil {
		tr.getLoggerWithName().Error(err, "Error publishing output")
	}
}

func (tr *Runner) getOutputSubject(channel string) string {
	outputSubject := viper.GetString(common.ConfigNatsOutputKey)
	if channel != "" {
		return fmt.Sprintf("%s.%s", outputSubject, channel)
	}

	return outputSubject
}

// prepareOutputMessage will check the length of the message and compress it if necessary.
// Fails on compressed messages bigger than the threshold.
func (tr *Runner) prepareOutputMessage(msg []byte) ([]byte, error) {
	maxSize, err := tr.getMaxMessageSize()
	if err != nil {
		return nil, fmt.Errorf("error getting max message size: %s", err) //nolint:goerr113 // error is wrapped
	}

	lenMsg := int64(len(msg))
	if lenMsg <= maxSize {
		return msg, nil
	}

	outMsg, err := common.CompressData(msg)
	if err != nil {
		return nil, err
	}

	lenOutMsg := int64(len(outMsg))
	if lenOutMsg > maxSize {
		tr.getLoggerWithName().V(1).Info("Compressed message size %s "+
			"exceeds maximum size allowed %s", sizeInMB(lenOutMsg), sizeInMB(maxSize))
		return nil, errors.ErrMessageToBig
	}

	tr.getLoggerWithName().Info(fmt.Sprintf("Message prepared with original size %s "+
		"and compressed size %s", sizeInMB(lenMsg), sizeInMB(lenOutMsg)))

	return outMsg, nil
}

func (tr *Runner) getResponseHandler(subject string) Handler {
	if responseHandler, ok := tr.responseHandlers[subject]; ok {
		return responseHandler
	}

	// returns the default response handler, or nil if it doesn't exist
	return tr.responseHandlers["default"]
}

func (tr *Runner) getMaxMessageSize() (int64, error) {
	streamInfo, err := tr.jetstream.StreamInfo(viper.GetString(common.ConfigNatsStreamKey))
	if err != nil {
		return 0, fmt.Errorf("error getting stream's max message size: %w", err)
	}

	streamMaxSize := int64(streamInfo.Config.MaxMsgSize)
	serverMaxSize := tr.nats.MaxPayload()

	if streamMaxSize == -1 {
		return serverMaxSize, nil
	}

	if streamMaxSize < serverMaxSize {
		return streamMaxSize, nil
	}

	return serverMaxSize, nil
}

func (tr *Runner) getMetricAttributes(requestID string) attribute.Set {
	return attribute.NewSet(
		attribute.KeyValue{
			Key:   "product",
			Value: attribute.StringValue(tr.sdk.Metadata.GetProduct()),
		},
		attribute.KeyValue{
			Key:   "version",
			Value: attribute.StringValue(tr.sdk.Metadata.GetVersion()),
		},
		attribute.KeyValue{
			Key:   "workflow",
			Value: attribute.StringValue(tr.sdk.Metadata.GetWorkflow()),
		},
		attribute.KeyValue{
			Key:   "process",
			Value: attribute.StringValue(tr.sdk.Metadata.GetProcess()),
		},
		attribute.KeyValue{
			Key:   "request_id",
			Value: attribute.StringValue(requestID),
		},
	)
}

func sizeInMB(size int64) string {
	mbSize := float32(size) / 1024 / 1024
	return fmt.Sprintf("%.1f MB", mbSize)
}
