package airbus_go

import (
	"bitbucket.mynt.myntra.com/plt/airbus-go/entry"
	"bitbucket.mynt.myntra.com/plt/airbus-go/producer"
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
