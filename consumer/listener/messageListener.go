package listener

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/avDec25/airbus-go/avro"
	"github.com/avDec25/airbus-go/constants"
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/logger"
	"github.com/avDec25/airbus-go/stats"
	goAvro "github.com/elodina/go-avro"
	"time"
)

var log = logger.GetLogger()

type messageListener struct {
	messageProcessor  entry.EventListener
	messageHProcessor entry.EventListenerHeaders
	messageCProcessor entry.EventListenerComplete
	event             *entry.EventEntry
	serviceUrl        string
	groupId           string
	appName           string
	schemaRegistry    []string
	numberOfRetries   int
	statsD            stats.StatsdCollector
	errorQProducer    entry.ErrorQProducer
}

func NewMessageListener(messageProcessor entry.EventListener, messageHProcessor entry.EventListenerHeaders, messageCProcessor entry.EventListenerComplete,
	event *entry.EventEntry, serviceUrl, groupId, appName string, schemaRegistry []string, retries int, statsD stats.StatsdCollector,
	errorQProducer entry.ErrorQProducer) entry.MessageListener {

	return &messageListener{
		messageProcessor:  messageProcessor,
		messageHProcessor: messageHProcessor,
		messageCProcessor: messageCProcessor,
		event:             event,
		serviceUrl:        serviceUrl,
		groupId:           groupId,
		appName:           appName,
		schemaRegistry:    schemaRegistry,
		numberOfRetries:   retries,
		statsD:            statsD,
		errorQProducer:    errorQProducer,
	}
}

func (this *messageListener) ProcessMessage(msg *sarama.ConsumerMessage, schemaType entry.SerializationSchema) {
	startTime := time.Now()
	i := 0
	var value interface{}
	var err error

RETRY:
	switch schemaType {
	case entry.Plain:
		value = string(msg.Value[:])
	case entry.Json:
		value = msg.Value
	case entry.Avro:
		url := this.schemaRegistry
		var recordValue interface{}
		for i := 0; i < len(url); i++ {
			if recordValue, err = avro.NewKafkaAvroDecoder(url[i]).DecodeByIdSchemaFallback(msg.Topic, msg.Value); err == nil {
				value = recordValue.(*goAvro.GenericRecord).Map()
				break
			}
		}
	default:
		err = errors.New(constants.ErrUnidentifiedSchema)
	}

	headers := make(map[string]string)
	if err == nil {
		for _, header := range msg.Headers {
			headers[string(header.Key[:])] = string(header.Value[:])
		}
		err = this.messageProcessor.OnEvent(string(msg.Key[:]), value)
		if err != nil {
			goto SkipToEnd
		}
		err = this.messageHProcessor.OnEvent(string(msg.Key[:]), value, headers)
		if err != nil {
			goto SkipToEnd
		}
		err = this.messageCProcessor.OnEvent(string(msg.Key[:]), value, msg.Topic, msg.Partition, msg.Offset,
			msg.Timestamp, msg.BlockTimestamp, headers)
	}
SkipToEnd:
	if err != nil && err.Error() != constants.ErrUnidentifiedSchema {
		i++
		if i <= this.numberOfRetries {
			log.Infof("Retrying processing for the event for client: %s and topic: %s-%s\n", this.appName,
				this.event.AppName, this.event.EventName)
			goto RETRY
		} else {
			this.handleConsumerError(err, msg.Key, msg.Value, headers)
		}
	}
	if this.statsD != nil {
		this.statsD.IncrementCounter(this.groupId + constants.SuccessAppender)
		elapsedTime := time.Since(startTime)
		this.statsD.Gauge(this.groupId+constants.MsgProcessAppender, elapsedTime.Nanoseconds()/1000/1000)
	}
	return
}

func (this *messageListener) handleConsumerError(err error, key, value []byte, headers map[string]string) {
	headers["X-Error-Message"] = err.Error()
	errorEvent := &entry.EventEntry{
		AppName: this.appName,
		Data:    string(value[:]),
		Headers: headers,
	}
	if len(key) > 0 {
		errorEvent.PartitionKey = string(key[:])
	}
	log.Errorf("Error Event: %+v, err: %s\n", errorEvent, err.Error())

	this.errorQProducer.Publish(errorEvent, this.event.EventName)
	if this.statsD != nil {
		this.statsD.IncrementCounter(this.groupId + constants.ErrorAppender)
	}
}
