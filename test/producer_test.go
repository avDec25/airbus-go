package test

import (
	"fmt"
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/logger"
	"github.com/op/go-logging"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	appNameProducerTest   = `test`
	eventNameProducerTest = `testevent`
	testDataProducerTest  = `Hello this is a test data.`
)

type listenableCallback struct{}

func TestSyncSend(t *testing.T) {
	logger.LogInit(logging.INFO)
	event := initProducerConfig(appNameProducerTest, eventNameProducerTest)
	prod, err := NewAirbusProducer(serviceUrlDC1, appNameProducerTest, config, nil, true)
	if err != nil {
		log.Error(err.Error())
		return
	}
	event.Data = testDataProducerTest
	for i := 0; i < 10; i++ {
		if _, _, err := prod.SyncSend(event); err != nil {
			assert.Nil(t, err, "Error should be nil")
		}
	}
	prod.Close()
}

func TestAsyncSend(t *testing.T) {
	event := initProducerConfig(appNameProducerTest, eventNameProducerTest)
	clientCallback := listenableCallback{}
	prod, err := NewAirbusProducer(serviceUrlDC1, appNameProducerTest, config, clientCallback, false)
	if err != nil {
		fmt.Println(err)
		return
	}
	event.Data = testDataProducerTest
	for i := 0; i < 10; i++ {
		if err := prod.AsyncSend(event); err != nil {
			assert.Nil(t, err, "Error should be nil")
		}
	}
	time.Sleep(time.Second * 10)
	prod.Close()
}

func (listenableCallback) OnSuccess(result *entry.Result) {
	fmt.Printf("Config: %+v\n", result)
}

func (listenableCallback) OnFailure(err error, msg *entry.Result) {
	fmt.Printf("Error: %s\n", err.Error())
}
