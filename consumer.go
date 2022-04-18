package airbus_go

import (
	"github.com/avDec25/airbus-go/consumer"
	"github.com/avDec25/airbus-go/consumer/cluster"
	"github.com/avDec25/airbus-go/entry"
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
