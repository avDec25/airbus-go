package test

import (
	"bytes"
	"errors"
	"github.com/avDec25/airbus-go/constants"
	"github.com/avDec25/airbus-go/consumer"
	"github.com/avDec25/airbus-go/consumer/cluster"
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/logger"
	"github.com/avDec25/airbus-go/producer"
	"github.com/avDec25/airbus-go/util"
	"math/rand"
	"strconv"
)

const pausePlayPath = `/bus/v2/consumer/register`
const serviceUrlDC1 = `http://platformairbus.stage.myntra.com`
const serviceUrlDC2 = `http://airbuskafka1:7071`

var config map[string]interface{}
var eventListenerEntities []*entry.EventListenerEntity
var eventList []string
var log = logger.GetLogger()

func NewAirbusConsumer(appName, serviceUrl string, config map[string]interface{}, eventListeners []*entry.EventListenerEntity) (chan bool, error) {
	airbusConsumer := &cluster.AirbusConsumer{
		AppName:        appName,
		ServiceUrl:     serviceUrl,
		Config:         config,
		EventListeners: eventListeners,
	}
	return consumer.NewConsumer(airbusConsumer)
}

func IsStringPresent(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func RandSeq(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func NewAirbusProducer(serviceUrl, appName string, config map[string]interface{},
	clientCallback entry.ListenableCallback, sync bool) (producer.Producer, error) {
	airbusProducer := &producer.AirbusProducer{
		AppName:        appName,
		ServiceUrl:     serviceUrl,
		Config:         config,
		ClientCallback: clientCallback,
		Sync:           sync,
	}
	return producer.NewProducer(airbusProducer)
}

func ChangePausePlay(serviceUrl, producerApp, eventName, consumerApp string, status bool) error {
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = constants.Authorization
	appEntries, err := util.GetConsumerEntity(serviceUrl, consumerApp)
	if err != nil {
		return err
	}
	if len(appEntries) == 0 {
		return errors.New(constants.AppEventNameNotFound)
	}
	appEntry := appEntries[0]
	var consumerEntry entry.ConsumerEventEntry
	for _, value := range appEntry.Consumes {
		if value.AppName == consumerApp && value.EventName == eventName {
			consumerEntry = value
			break
		}
	}
	//start of request
	requestJson := `{"appName":"` + consumerApp + `","consumerEvents":[{"isSubscribed":` + strconv.FormatBool(status) + `,"appName":"` + producerApp + `","eventName":"` + eventName + `","consumerType":"` + consumerEntry.ConsumerType + `","isBCPCompliant":` + strconv.FormatBool(consumerEntry.IsBCPCompliant) + `}]}`
	//end of request
	request := []byte(requestJson)
	_, err = util.HttpPut(serviceUrl+pausePlayPath, headers, bytes.NewBuffer(request))
	return err
}

func initProducerConfig(appName, eventName string) *entry.EventEntry {
	config = make(map[string]interface{})
	config["batch.size"] = 5
	config["batch.timeout"] = 100     // 100 ms
	config["request.timeout"] = 10000 // 10000 ms

	var event = entry.EventEntry{
		AppName:   appName,
		EventName: eventName,
	}
	return &event
}

func initConsumerConfig(appNames, eventNames []string, listener entry.EventListener) {
	eventListenerEntities = eventListenerEntities[:0]
	eventList = eventList[:0]
	for i, value := range appNames {
		eventListenerEntities = append(eventListenerEntities,
			&entry.EventListenerEntity{
				AirbusEvent: &entry.EventEntry{
					AppName:   value,
					EventName: eventNames[i],
				},
				AirbusEventListener: listener,
				ConcurrentListeners: 10,
				AutoCommitDisable:   false,
			})
	}
	config = make(map[string]interface{})
	config["auto.offset.reset"] = -1
	config["commit.interval"] = 5
}

func initConsumerConfigWithHeader(appNames, eventNames []string, listener entry.EventListener, headerListener entry.EventListenerHeaders) {
	eventListenerEntities = eventListenerEntities[:0]
	eventList = eventList[:0]
	for i, value := range appNames {
		eventListenerEntities = append(eventListenerEntities,
			&entry.EventListenerEntity{
				AirbusEvent: &entry.EventEntry{
					AppName:   value,
					EventName: eventNames[i],
				},
				AirbusEventListener:        listener,
				AirbusHeadersEventListener: headerListener,
				ConcurrentListeners:        10,
				AutoCommitDisable:          false,
			})
	}
	config = make(map[string]interface{})
	config["auto.offset.reset"] = -1
	config["commit.interval"] = 5
}
