package test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	producerAppNameReplicationTest = `kohli`
	consumerAppNameReplicationTest = `kohli`
	newEventNameReplicationTest    = `sachin`
)

type eventListenerReplicationTest struct{}

func TestReplication(t *testing.T) {
	// Initializing the producer
	event := initProducerConfig(producerAppNameReplicationTest, newEventNameReplicationTest)
	prod, err := NewAirbusProducer(serviceUrlDC1, producerAppNameReplicationTest, config, nil, true)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Initializing the consumer
	appNames := []string{producerAppNameReplicationTest}
	eventNames := []string{newEventNameReplicationTest}
	initConsumerConfig(appNames, eventNames, &eventListenerReplicationTest{})
	_, err = NewAirbusConsumer(consumerAppNameReplicationTest, serviceUrlDC2, config, eventListenerEntities)
	if err != nil {
		fmt.Println("Error:", err.Error())
		assert.Nil(t, err, "Error should be nil")
		return
	}
	time.Sleep(5 * time.Second)

	log.Info("Sending events.")
	eventData := RandSeq(20)
	event.Data = eventData
	if _, _, err := prod.SyncSend(event); err != nil {
		assert.Nil(t, err, "Error should be nil")
	}
	time.Sleep(30 * time.Second)                          //Waiting for consumer to receive the event
	assert.True(t, IsStringPresent(eventData, eventList)) //asserting if it is present or not
}

func (eventListenerReplicationTest) OnEvent(key string, value interface{}) error {
	log.Info("onevent" + fmt.Sprintf("Value: %+v", value))
	eventList = append(eventList, fmt.Sprintf("%+v", value))
	return nil
}
