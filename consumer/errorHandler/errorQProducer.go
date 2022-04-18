package errorHandler

import (
	"github.com/avDec25/airbus-go/constants"
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/logger"
	"github.com/avDec25/airbus-go/producer"
	"strings"
	"sync"
)

var log = logger.GetLogger()
var errorQProducerCache = make(map[string]*errorQProducer)
var lock sync.Mutex

type errorQProducer struct {
	producer producer.Producer
}

func NewErrorQProducer(serviceUrl, appName string) entry.ErrorQProducer {
	lock.Lock()
	defer lock.Unlock()

	if errorProd, exists := errorQProducerCache[appName]; exists {
		return errorProd
	} else {
		airbusProducer := &producer.AirbusProducer{
			ServiceUrl: serviceUrl,
			AppName:    appName,
		}
		prod, err := producer.NewProducer(airbusProducer)
		if err != nil {
			log.Error(err.Error())
			return nil
		}
		errorProd = &errorQProducer{
			producer: prod,
		}
		errorQProducerCache[appName] = errorProd
		return errorProd
	}
}

func (this *errorQProducer) Publish(errorEvent *entry.EventEntry, originalEventName string) {
	if originalEventName != "" && strings.Contains(originalEventName, constants.ErrorQueueAppender) {
		errorEvent.EventName = originalEventName
	} else {
		errorEvent.EventName = originalEventName + constants.ErrorQueueAppender
	}
	if err := this.producer.AsyncSend(errorEvent); err != nil {
		log.Error(err.Error())
	}
}
