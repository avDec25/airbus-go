package airbus_go

import (
	"bitbucket.mynt.myntra.com/plt/airbus-go/consumer"
	"bitbucket.mynt.myntra.com/plt/airbus-go/consumer/cluster"
	"bitbucket.mynt.myntra.com/plt/airbus-go/entry"
)

func NewAirbusConsumer(appName, serviceUrl string, config map[string]interface{}, eventListeners []*entry.EventListenerEntity) (chan bool, error) {
	airbusConsumer := &cluster.AirbusConsumer{
		AppName:        appName,
		ServiceUrl:     serviceUrl,
		Config:         config,
		EventListeners: eventListeners,
	}
	return consumer.NewConsumer(airbusConsumer)
}
