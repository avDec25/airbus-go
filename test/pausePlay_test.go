package test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	producerAppNamePausePlay = `test`
	consumerAppNamePausePlay = `test`
	eventNamePausePlay       = `testevent`
)

type eventListenerPausePlay struct{}

func TestPausePlayScenario(t *testing.T) {
	log.Info("Playing the event") // Playing the event
	err := ChangePausePlay(serviceUrlDC1, producerAppNamePausePlay, eventNamePausePlay, consumerAppNamePausePlay, true)
	assert.Nil(t, err, "Error should be nil")

	//Initializing the producer
	event := initProducerConfig(producerAppNamePausePlay, eventNamePausePlay)
	prod, err := NewAirbusProducer(serviceUrlDC1, producerAppNamePausePlay, config, nil, true)
	if err != nil {
		fmt.Println(err)
		return
	}
	//Initializing the consumer
	appNames := []string{producerAppNamePausePlay}
	eventNames := []string{eventNamePausePlay}
	initConsumerConfig(appNames, eventNames, &eventListenerPausePlay{})
	_, err = NewAirbusConsumer(consumerAppNamePausePlay, serviceUrlDC1, config, eventListenerEntities)
	if err != nil {
		fmt.Println("Error:", err.Error())
		assert.Nil(t, err, "Error should be nil")
		return
	}
	time.Sleep(10 * time.Second) // Waiting for the consumer to come up

	log.Info("Sending event.") // Sending event
	testData := RandSeq(20)
	event.Data = testData
	if _, _, err := prod.SyncSend(event); err != nil {
		assert.Nil(t, err, "Error should be nil")
	}
	time.Sleep(10 * time.Second)                         //Waiting for consumer to receive the event
	assert.True(t, IsStringPresent(testData, eventList)) //asserting if it is present or not

	log.Info("Pausing the event.") //Pausing the event
	err = ChangePausePlay(serviceUrlDC1, producerAppNamePausePlay, eventNamePausePlay, consumerAppNamePausePlay, false)
	assert.Nil(t, err, "Error should be nil")
	time.Sleep(20 * time.Second) //Waiting for consumer to stop

	log.Info("Sending the event.") // Sending event again
	testData = RandSeq(20)
	event.Data = testData
	if _, _, err := prod.SyncSend(event); err != nil {
		assert.Nil(t, err, "Error should be nil")
	}
	time.Sleep(10 * time.Second)                          //Waiting for consumer to check whether received the event
	assert.False(t, IsStringPresent(testData, eventList)) //asserting if it is present or not

	log.Info("Playing the event") //Playing the event
	err = ChangePausePlay(serviceUrlDC1, producerAppNamePausePlay, eventNamePausePlay, consumerAppNamePausePlay, true)
	assert.Nil(t, err, "Error should be nil")
	time.Sleep(30 * time.Second)                         //Waiting for consumer to start
	assert.True(t, IsStringPresent(testData, eventList)) //asserting if it is present or not

	prod.Close()
}

func (eventListenerPausePlay) OnEvent(key string, value interface{}) error {
	log.Info("onevent" + fmt.Sprintf("Value: %+v", value))
	eventList = append(eventList, fmt.Sprintf("%+v", value))
	return nil
}
