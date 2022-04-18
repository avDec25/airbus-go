package test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	producerAppNameConsumerTest = `test`
	consumerAppNameConsumerTest = `test`
	eventNameConsumerTest       = `testevent`
	headerKeyConsumerTest       = `headerKeyConsumerTest`
)

type eventListenerConsumerTest struct{}
type headersEventListenerConsumerTest struct{}

// Test about a simple consumer checking functionality of both header and value
func TestBasicConsumer(t *testing.T) {
	event := initProducerConfig(producerAppNameConsumerTest, eventNameConsumerTest)
	prod, err := NewAirbusProducer(serviceUrlDC1, producerAppNameConsumerTest, config, nil, true)
	if err != nil {
		fmt.Println(err)
		return
	}

	//Initializing the consumer
	appNames := []string{producerAppNameConsumerTest}
	eventNames := []string{eventNameConsumerTest}
	initConsumerConfigWithHeader(appNames, eventNames, &eventListenerConsumerTest{}, &headersEventListenerConsumerTest{})
	_, err = NewAirbusConsumer(consumerAppNameConsumerTest, serviceUrlDC1, config, eventListenerEntities)
	if err != nil {
		fmt.Println("Error:", err.Error())
		assert.Nil(t, err, "Error should be nil")
		return
	}
	time.Sleep(5 * time.Second)
	// fmt.Printf("%s %s", eventData, eventList)
	// Sending the event
	log.Info("Sending events.")
	eventData := RandSeq(20)
	event.Data = eventData
	headerData := RandSeq(20)
	headers := make(map[string]string)
	headers[headerKeyConsumerTest] = headerData
	event.Headers = headers
	if _, _, err := prod.SyncSend(event); err != nil {
		assert.Nil(t, err, "Error should be nil")
	}
	prod.Close()
	time.Sleep(25 * time.Second)
	assert.True(t, IsStringPresent(eventData, eventList))
	// assert.True(t, IsStringPresent(headerData, eventList))
}

func (eventListenerConsumerTest) OnEvent(key string, value interface{}) error {
	log.Info("onevent" + fmt.Sprintf("Value: %+v", value))
	eventList = append(eventList, fmt.Sprintf("%+v", value))
	return nil
}

func (headersEventListenerConsumerTest) OnEvent(key string, value interface{}, headers map[string]string) error {
	if headerValue, ok := headers[headerKeyConsumerTest]; ok {
		eventList = append(eventList, headerValue)
		log.Info("OnHeaderEvent" + fmt.Sprintf("Value: %+v", headerValue))
	} else {
		log.Error("No header is found")
	}
	log.Info("onevent" + fmt.Sprintf("Value: %+v", value))
	eventList = append(eventList, fmt.Sprintf("%+v", value))
	return nil
}
