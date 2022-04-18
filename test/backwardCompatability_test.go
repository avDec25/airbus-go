package test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	producerAppNameBackwardsTest = `kohli`
	consumerAppNameBackwardsTest = `kohli`
	oldEventNameBackwardsTest    = `dravid`
	newEventNameBackwardsTest    = `sachin`
)

type eventListenerBackwardsTest struct{}

func TestBackwardsCompatibility(t *testing.T) {
	// Initializing the producers
	oldEvent := initProducerConfig(producerAppNameBackwardsTest, oldEventNameBackwardsTest)
	newEvent := initProducerConfig(producerAppNameBackwardsTest, newEventNameBackwardsTest)
	prod, err := NewAirbusProducer(serviceUrlDC1, producerAppNameBackwardsTest, config, nil, true)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Initializing the consumer
	appNames := []string{producerAppNameBackwardsTest, producerAppNameBackwardsTest}
	eventNames := []string{oldEventNameBackwardsTest, newEventNameBackwardsTest}
	initConsumerConfig(appNames, eventNames, &eventListenerBackwardsTest{})

	_, err = NewAirbusConsumer(consumerAppNameBackwardsTest, serviceUrlDC1, config, eventListenerEntities)
	if err != nil {
		fmt.Println("Error:", err.Error())
		assert.Nil(t, err, "Error should be nil")
		return
	}
	time.Sleep(5 * time.Second)

	log.Info("Sending events.")
	oldEventData := RandSeq(20)
	newEventData := RandSeq(20)
	oldEvent.Data = oldEventData
	newEvent.Data = newEventData
	if _, _, err := prod.SyncSend(oldEvent); err != nil {
		assert.Nil(t, err, "Error should be nil")
	}
	if _, _, err := prod.SyncSend(newEvent); err != nil {
		assert.Nil(t, err, "Error should be nil")
	}
	time.Sleep(5 * time.Second)                              //Waiting for consumer to receive the event
	assert.True(t, IsStringPresent(oldEventData, eventList)) //asserting if it is present or not
	assert.True(t, IsStringPresent(newEventData, eventList)) //asserting if it is present or not
}

func (eventListenerBackwardsTest) OnEvent(key string, value interface{}) error {
	log.Info("onevent" + fmt.Sprintf("Value: %+v", value))
	eventList = append(eventList, fmt.Sprintf("%+v", value))
	return nil
}
