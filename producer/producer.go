package producer

import (
	"bitbucket.mynt.myntra.com/plt/airbus-go/constants"
	"bitbucket.mynt.myntra.com/plt/airbus-go/entry"
	"bitbucket.mynt.myntra.com/plt/airbus-go/stats"
	"bitbucket.mynt.myntra.com/plt/airbus-go/util"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Producer interface {
	AsyncSend(event *entry.EventEntry) error
	SyncSend(event *entry.EventEntry) (int32, int64, error)
	Close()
}

type producer struct {
	hosts               []string
	schemaRegistry      []string
	config              *sarama.Config
	serviceUrl          string
	statsD              stats.StatsdCollector
	asyncAirbusProducer sarama.AsyncProducer
	syncAirbusProducer  sarama.SyncProducer
	sync                bool
	clientProvidedAirbusConfig *AirbusProducer
	
}

const (
	serverKey         = "bootstrap.servers"
	ackKey            = "acks"
	retryKey          = "retries"
	batchKey          = "batch.size"
	frequencyKey      = "batch.timeout" // in milliseconds
	clientKey         = "client.id"
	schemaRegistryKey = "schemaRegistryURL"
	requestTimeoutKey = "request.timeout" // in milliseconds
	compressionKey    = "compression.type"
	CompressionNone   = "none"
	CompressionGZIP   = "gzip"
	CompressionLZ4    = "lz4"
	CompressionSnappy = "snappy"
)

func NewProducer(airbusProducer *AirbusProducer) (Producer, error) {
	if airbusProducer.ServiceUrl == "" {
		return nil, errors.New(constants.EmptyServiceUrl)
	}

	config, err := util.GetProducerConfig(airbusProducer.ServiceUrl)
	if err != nil {
		return nil, err
	}

	if len(airbusProducer.Config) > 0 {
		for key, value := range airbusProducer.Config {
			switch key {
			case serverKey:
				continue
			default:
				config[key] = value
			}
		}
	}

	p := &producer{
		config:         sarama.NewConfig(),
		serviceUrl:     airbusProducer.ServiceUrl,
		statsD:         stats.GetProducerStatsdClient(airbusProducer.ServiceUrl),
	}
	p.clientProvidedAirbusConfig = airbusProducer
	for key, value := range config {
		if value != nil {
			switch value.(type) {
			case string:
				switch key {
				case serverKey:
					p.hosts = strings.Split(value.(string), ",")
				case ackKey:
					if acks, err := strconv.ParseInt(value.(string), 10, 16); err != nil {
						return nil, err
					} else {
						p.config.Producer.RequiredAcks = sarama.RequiredAcks(acks)
					}
				case retryKey:
					if retries, err := strconv.ParseInt(value.(string), 10, 0); err != nil {
						return nil, err
					} else {
						p.config.Producer.Retry.Max = int(retries)
					}
				case batchKey:
					if batchSize, err := strconv.ParseInt(value.(string), 10, 0); err != nil {
						return nil, err
					} else {
						p.config.Producer.Flush.Messages = int(batchSize)
					}
				case frequencyKey:
					if frequency, err := strconv.ParseInt(value.(string), 10, 0); err != nil {
						return nil, err
					} else {
						p.config.Producer.Flush.Frequency = time.Duration(frequency) * time.Millisecond
					}
				case clientKey:
					p.config.ClientID = value.(string)
				case schemaRegistryKey:
					p.schemaRegistry = strings.Split(value.(string), ",")
				case requestTimeoutKey:
					if requestTimeout, err := strconv.ParseInt(value.(string), 10, 0); err != nil {
						return nil, err
					} else {
						p.config.Producer.Timeout = time.Duration(requestTimeout) * time.Millisecond
					}
				case compressionKey:
					switch value {
					case CompressionNone:
						p.config.Producer.Compression = sarama.CompressionNone
					case CompressionGZIP:
						p.config.Producer.Compression = sarama.CompressionGZIP
					case CompressionSnappy:
						p.config.Producer.Compression = sarama.CompressionSnappy
					case CompressionLZ4:
						p.config.Producer.Compression = sarama.CompressionLZ4
					}
				}

			case int:
				switch key {
				case ackKey:
					p.config.Producer.RequiredAcks = sarama.RequiredAcks(value.(int))
				case retryKey:
					p.config.Producer.Retry.Max = value.(int)
				case batchKey:
					p.config.Producer.Flush.Messages = value.(int)
				case frequencyKey:
					p.config.Producer.Flush.Frequency = time.Duration(value.(int)) * time.Millisecond
				case requestTimeoutKey:
					p.config.Producer.Timeout = time.Duration(value.(int)) * time.Millisecond
				}
			}
		}
	}

	p.sync = airbusProducer.Sync
	p.config.Producer.Return.Errors = true
	p.config.Producer.Return.Successes = true
	p.config.Version = constants.KafkaVersion

	// if airbusProducer.Sync {
	// 	p.saramaProducer = getSyncAirbusProducer(p.hosts, p.config, stats.GetProducerStatsdClient(airbusProducer.ServiceUrl))
	// 	p.sync = true
	// } else {
	// 	p.saramaProducer = getAsyncAirbusProducer(p.hosts, p.config,airbusProducer.ClientCallback, stats.GetProducerStatsdClient(airbusProducer.ServiceUrl))
		
	// }

	log.Infof("Producer Config: %+v\n", p.config.Producer)
	go p.addShutdownHook()
	return p, nil
}

// Shutdown client gracefully on system events
func (this *producer) addShutdownHook() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Printf("Producer client shutting down... Signal: %v\n", sig)
		this.shutdown()
		done <- true
	}()
	<-done

	close(sigs)
	close(done)
}

// Sends the message after checking topic schema type
func (this *producer) AsyncSend(event *entry.EventEntry) error {
	if this.sync {
		return errors.New(constants.SyncProducerError)
	}
	_,_,r:= this.sendHandle(event)
	return r
}

func (this *producer) sendHandle(event *entry.EventEntry) (int32, int64, error) {
	topicNameWithoutPrefix, err := util.GetTopicName("", event.AppName, event.EventName)
	if err != nil {
		return 0, 0, err
	}

	topicEntry, err := util.GetTopicDetailsCache(this.serviceUrl, topicNameWithoutPrefix)
	if err != nil {
		log.Error(err.Error())
		return 0, 0, errors.New(constants.ErrorLoadingTopicFromCache)
	}

	// fetch topic to produce to based on bcp compliance
	topicName, err := getTopicName(this.serviceUrl, topicEntry)
	if err != nil {
		log.Error(err.Error())
		return 0, 0, err
	}
	if entry.Subscribe == topicEntry.Status {
		switch topicEntry.SchemaType {
		case entry.Plain:
			return this.send(topicName, event)
		case entry.Json:
			return this.validateAndSend(topicName, topicEntry, event)
		case entry.Avro:
			return this.sendAvro(topicName, topicEntry, event)
		default:
			return 0, 0, errors.New(constants.ErrUnidentifiedSchema)
		}
	} else {
		return 0, 0, errors.New(constants.TopicUnsubscribed)
	}
}
func (this *producer) SyncSend(event *entry.EventEntry) (int32, int64, error) {
	if !this.sync {
		return 0, 0, errors.New(constants.AsyncProducerError)
	}
	return this.sendHandle(event)
	
}

func (this *producer) Close() {
	GetProducerFactoryInstance().clean()
}

// based on bcp compliance get topic name for a given topic entry details, service url that it should produce to
func getTopicName(serviceUrl string, topicEntry *entry.TopicEntry) (string, error) {
	topicName, _ := util.GetTopicName("", topicEntry.AppName, topicEntry.EventName)
	if topicEntry.IsBCPCompliant {
		topicName, err := util.GetTopicName(serviceUrl, topicEntry.AppName, topicEntry.EventName)
		return topicName, err
	}
	return topicName, nil
}
