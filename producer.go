package airbus_go

import (
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/producer"
)

func NewAirbusProducer(serviceUrl, appName string, config map[string]interface{}, clientCallback entry.ListenableCallback, sync bool) (producer.Producer, error) {
	airbusProducer := &producer.AirbusProducer{
		AppName:        appName,
		ServiceUrl:     serviceUrl,
		Config:         config,
		ClientCallback: clientCallback,
		Sync:           sync,
	}
	return producer.NewProducer(airbusProducer)
}
