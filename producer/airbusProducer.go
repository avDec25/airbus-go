package producer

import (
	"github.com/Shopify/sarama"
	"github.com/avDec25/airbus-go/constants"
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/stats"
	"sync"
)

type AirbusProducer struct {
	AppName        string
	ServiceUrl     string
	Config         map[string]interface{}
	ClientCallback entry.ListenableCallback
	Sync           bool
}

type SaramaSyncProducer struct {
	sarama.SyncProducer
	statsD stats.StatsdCollector
}
type SaramaAsyncProducer struct {
	sarama.AsyncProducer
	wg             sync.WaitGroup
	close          chan bool
	clientCallback entry.ListenableCallback
	statsD         stats.StatsdCollector
}

type SaramaProducer interface {
	Send(message *sarama.ProducerMessage) (int32, int64, error)
	Close()
}

func (this SaramaAsyncProducer) Send(message *sarama.ProducerMessage) (int32, int64, error) {
	this.Input() <- message
	return 0, 0, nil
}
func (this SaramaAsyncProducer) Close() {
	this.AsyncClose()
	this.close <- true
	//wait for the producer to close
	this.wg.Wait()
	if this.statsD != nil {
		this.statsD.Close()
	}
}

func (this SaramaSyncProducer) Send(message *sarama.ProducerMessage) (int32, int64, error) {
	//
	partition, offset, err := this.SendMessage(message)
	if err != nil {
		log.Errorf("Unable to send message: %+v due to error: %s\n", message, err.Error())
		if this.statsD != nil {
			this.statsD.IncrementCounter(message.Topic + constants.FailureAppender)
		}
	} else {
		log.Debugf("Sent message: %s with offset: %d\n", message.Value, offset)
		if this.statsD != nil {
			this.statsD.IncrementCounter(message.Topic + constants.SuccessAppender)
		}
	}
	return partition, offset, err
}

func (this SaramaSyncProducer) Close() {
	this.SyncProducer.Close()
	if this.statsD != nil {
		this.statsD.Close()
	}
}
