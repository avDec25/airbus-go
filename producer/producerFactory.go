package producer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/avDec25/airbus-go/constants"
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/stats"
	"github.com/avDec25/airbus-go/util"
	"strings"
	"sync"
	"time"
)

var once sync.Once
var lock = sync.RWMutex{}

//gives a producer instance based on appname, eventname, sync param
type ProducerFactory interface {
	getProducer(appName string, eventName string, sync bool, hosts []string, conf *sarama.Config, clientCallback entry.ListenableCallback, serviceUrl string) SaramaProducer
	clean()
}
type ProducerFactoryImpl struct {
	store map[producerKey]SaramaProducer
}

type producerKey struct {
	appName   string
	eventName string
	sync      bool
}

var instance *ProducerFactoryImpl

func GetProducerFactoryInstance() *ProducerFactoryImpl {

	once.Do(func() {
		instance = &ProducerFactoryImpl{make(map[producerKey]SaramaProducer)}
	})

	return instance
}

func (this *ProducerFactoryImpl) getProducer(appName string, eventName string, sync bool, conf *sarama.Config, clientCallback entry.ListenableCallback, serviceUrl string) (SaramaProducer, error) {
	lock.RLock()
	producer, ok := this.store[producerKey{appName, eventName, sync}]
	if ok {
		lock.RUnlock()
		return producer, nil
	}
	lock.RUnlock()
	//upgrading to write lock as we are going to add an entry in map
	lock.Lock()
	defer lock.Unlock()

	hosts, err := this.getHostList(appName, eventName, serviceUrl)

	if err != nil {
		return nil, fmt.Errorf("Error getting hostlist: %s", err)
	}
	if conf.ClientID == constants.DefaultClientId {
		conf.ClientID = appName + "-" + eventName
	}

	this.store[producerKey{appName, eventName, sync}] = this.createProducer(hosts, conf, serviceUrl, clientCallback, sync)
	return this.store[producerKey{appName, eventName, sync}], nil
}

func (this *ProducerFactoryImpl) clean() {
	lock.Lock()
	defer lock.Unlock()
	for k, v := range this.store {
		v.Close()
		delete(this.store, k)
	}
}
func (this *ProducerFactoryImpl) createProducer(hosts []string, conf *sarama.Config, serviceUrl string, clientCallback entry.ListenableCallback, sync bool) SaramaProducer {
	if sync {
		return getSyncAirbusProducer(hosts, conf, stats.GetProducerStatsdClient(serviceUrl))
	}
	return getAsyncAirbusProducer(hosts, conf, clientCallback, stats.GetProducerStatsdClient(serviceUrl))

}

func (this *ProducerFactoryImpl) getHostList(appName string, eventName string, serviceUrl string) ([]string, error) {
	topicNameWithoutPrefix, err := util.GetTopicName("", appName, eventName)
	if err != nil {
		return nil, fmt.Errorf("Error getting topicName: %s", err)
	}

	topicEntry, err := util.GetTopicDetailsCache(serviceUrl, topicNameWithoutPrefix)
	if err != nil {
		return nil, fmt.Errorf("Error getting GetTopicDetailsCache: %s", err)
	}

	hostslist, err := util.GetBootstrapServers(serviceUrl, topicEntry.ClusterId)
	if err != nil {
		return nil, fmt.Errorf("Error getting GetBootstrapServers: %s", err)
	}
	return strings.Split(hostslist, ","), nil
}

// Async kafka producer.
// It is user's responsibility to close the producer after use
func getAsyncAirbusProducer(hosts []string, conf *sarama.Config, clientCallback entry.ListenableCallback, statsD stats.StatsdCollector) SaramaAsyncProducer {
RECONNECT:
	producer, err := sarama.NewAsyncProducer(hosts, conf)
	if err != nil {
		log.Errorf("Error while initiating Kafka producer %s", err)
		time.Sleep(constants.BackoffTime)
		goto RECONNECT
	}
	var wg sync.WaitGroup
	saramaProducer := SaramaAsyncProducer{producer, wg, make(chan bool, 1), clientCallback, statsD}
	log.Info("Kafka Producer connected")
	onSend(saramaProducer)
	return saramaProducer
}

// Sync kafka producer.
// It is user's responsibility to close the producer after use
func getSyncAirbusProducer(hosts []string, conf *sarama.Config, statsD stats.StatsdCollector) SaramaSyncProducer {
RECONNECT:
	producer, err := sarama.NewSyncProducer(hosts, conf)
	if err != nil {
		log.Errorf("Error while initiating Kafka producer %s", err)
		time.Sleep(constants.BackoffTime)
		goto RECONNECT
	}
	log.Info("Kafka Producer connected")
	saramaProducer := SaramaSyncProducer{producer, statsD}
	return saramaProducer
}

// After the producer sends a message trigger onSuccess or onFailure
func onSend(producer SaramaAsyncProducer) {
	producer.wg.Add(1)
	go func() {
		defer producer.wg.Done()
		var sDone, eDone, done bool

		for !(sDone && eDone && done) {
			select {
			case result, ok := <-producer.Successes():
				if ok {
					log.Debugf("Sent message: %s with offset: %d\n", result.Value, result.Offset)
					if producer.statsD != nil {
						producer.statsD.IncrementCounter(result.Topic + constants.SuccessAppender)
					}
					if producer.clientCallback != nil {
						producer.clientCallback.OnSuccess(&entry.Result{
							Topic:     result.Topic,
							Key:       result.Key,
							Value:     result.Value,
							Timestamp: result.Timestamp,
							Offset:    result.Offset,
							Partition: result.Partition,
						})
					}
				} else {
					sDone = true
				}

			case err, ok := <-producer.Errors():
				if ok {
					log.Errorf("Unable to send message: %+v due to error: %s\n", err.Msg, err.Err.Error())
					if producer.statsD != nil {
						producer.statsD.IncrementCounter(err.Msg.Topic + constants.FailureAppender)
					}
					if producer.clientCallback != nil {
						producer.clientCallback.OnFailure(err.Err, &entry.Result{
							Topic:     err.Msg.Topic,
							Key:       err.Msg.Key,
							Value:     err.Msg.Value,
							Timestamp: err.Msg.Timestamp,
							Offset:    err.Msg.Offset,
							Partition: err.Msg.Partition,
						})
					}
				} else {
					eDone = true
				}

			case <-producer.close:
				done = true
			}
		}
	}()
}
