package consumer

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/avDec25/airbus-go/constants"
	airbusCluster "github.com/avDec25/airbus-go/consumer/cluster"
	"github.com/avDec25/airbus-go/consumer/errorHandler"
	"github.com/avDec25/airbus-go/consumer/executor"
	"github.com/avDec25/airbus-go/consumer/metadata"
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/logger"
	"github.com/avDec25/airbus-go/stats"
	"github.com/avDec25/airbus-go/util"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var log = logger.GetLogger()
var metaOnce sync.Once

type consumer struct {
	hosts          []string
	schemaRegistry []string
	serviceUrl     string
	appName        string
	clientConfig   map[string]interface{}
	eventListeners []*entry.EventListenerEntity
	statsD         stats.StatsdCollector
	errorQProducer entry.ErrorQProducer
	metadataCache  entry.ConsumerMetadataCache
	wg             sync.WaitGroup
	cShutdown      chan bool
}

const (
	serverKey         = "bootstrap.servers"
	offsetKey         = "auto.offset.reset" // -2: oldest-offset and -1: newest-offset
	commitIntervalKey = "commit.interval"   // in seconds
	clientKey         = "client.id"
	groupKey          = "group.id"
	schemaRegistryKey = "schemaRegistryURL"
	concurrencyKey    = "max.poll.records"
)

func NewConsumer(airbusConsumer *airbusCluster.AirbusConsumer) (chan bool, error) {
	if airbusConsumer.ServiceUrl == "" {
		return nil, errors.New(constants.EmptyServiceUrl)
	}
	if airbusConsumer.AppName == "" {
		return nil, errors.New(constants.ErrorAppNameNotFound)
	}
	if len(airbusConsumer.EventListeners) == 0 {
		return nil, errors.New(constants.ErrorEmptyEventListeners)
	}

	c := &consumer{
		serviceUrl:     airbusConsumer.ServiceUrl,
		appName:        airbusConsumer.AppName,
		eventListeners: airbusConsumer.EventListeners,
		statsD:         stats.GetConsumerStatsdClient(airbusConsumer.ServiceUrl),
		errorQProducer: errorHandler.NewErrorQProducer(airbusConsumer.ServiceUrl, airbusConsumer.AppName),
		metadataCache:  metadata.GetConsumerCacheInstance(),
		cShutdown:      make(chan bool, 1),
	}
	if err := c.isValid(); err != nil {
		return nil, err
	}

	config, err := util.GetConsumerConfig(airbusConsumer.ServiceUrl)
	if err != nil {
		return nil, err
	}

	if len(airbusConsumer.Config) > 0 {
		for key, value := range airbusConsumer.Config {
			switch key {
			case serverKey:
				continue
			default:
				config[key] = value
			}
		}
	}
	c.clientConfig = config

	go c.addShutdownHook()
	err = c.startExecution()
	if err != nil {
		return nil, err
	}
	return c.cShutdown, nil
}

// Shutdown client gracefully on system events
func (this *consumer) addShutdownHook() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Printf("Consumer client shutting down... Signal: %v\n", sig)
		this.shutdown()
		done <- true
	}()
	<-done

	close(sigs)
	close(done)
}

func (this *consumer) startExecution() (err error) {
	go this.initializeMetadataListener()

	var subscribedEvents []interface{}
	if subscribedEvents, err = this.getSubscribedEvents(); err != nil {
		return
	}
	for _, eventListener := range this.eventListeners {
		var topicName string
		topicName, err = util.GetTopicName("", eventListener.AirbusEvent.AppName, eventListener.AirbusEvent.EventName)
		if err != nil {
			return
		}
		if util.Contains(topicName, subscribedEvents) {
			if err = this.initializeBCPListener(eventListener, this.clientConfig, true); err != nil {
				return
			}
		} else {
			log.Infof("Consumer is not subscribed (it is in pause state) to this event: %s. "+
				"Please put in play state to start listening\n", topicName)
			if err = this.initializeBCPListener(eventListener, this.clientConfig, false); err != nil {
				return
			}
		}
	}

	err = this.initializeErrorListeners()
	return
}

func (this *consumer) getSubscribedEvents() ([]interface{}, error) {
	data, err := util.GetConsumerEntity(this.serviceUrl, this.appName)
	if err != nil {
		return nil, err
	}
	appEntry := data[0]
	var subscribedEvents []interface{}
	for _, event := range appEntry.Consumes {
		topicName, err := util.GetTopicName("", event.AppName, event.EventName)
		if err != nil {
			return nil, err
		}
		if event.IsSubscribed {
			subscribedEvents = append(subscribedEvents, topicName)
		}
	}
	return subscribedEvents, nil
}

func (this *consumer) initializeMetadataListener() {
	metaOnce.Do(func() {
		airbusConsumerRegistrationEvent := &entry.EventEntry{
			AppName:   constants.ConsumerRegisterationAppName,
			EventName: constants.ConsumerRegisterationEventName,
		}
		metadataListener := entry.EventListenerEntity{
			AirbusEvent:         airbusConsumerRegistrationEvent,
			AirbusEventListener: metadata.NewConsumerRegistrationMetadataListener(this.appName, this.metadataCache),
			ConcurrentListeners: 1,
		}

		metadataConsumerConfig := util.GetMetadataConsumerConfig()
		registrationClientConfig := make(map[string]interface{})
		for key, value := range this.clientConfig {
			switch key {
			case offsetKey:
				registrationClientConfig[key] = sarama.OffsetNewest
			default:
				registrationClientConfig[key] = value
			}
		}
		if len(metadataConsumerConfig) > 0 {
			for key, value := range metadataConsumerConfig {
				switch key {
				case serverKey:
					continue
				default:
					registrationClientConfig[key] = value
				}
			}
		}

		topic, err := util.GetTopicName("", constants.ConsumerRegisterationAppName, constants.ConsumerRegisterationEventName)
		if err != nil {
			log.Errorf(constants.ErrorMetadataAirbus, err.Error())
		}
		err = this.initializeListener(&metadataListener, topic, registrationClientConfig, true)
		if err != nil {
			log.Errorf(constants.ErrorMetadataAirbus, err.Error())
		}
	})
}

func (this *consumer) initializeErrorListeners() error {
	for _, event := range this.eventListeners {
		errorEntry := &entry.EventEntry{
			AppName:   this.appName,
			EventName: event.AirbusEvent.EventName + constants.ErrorQueueAppender,
		}
		errorEntity := entry.EventListenerEntity{
			AirbusEvent:         errorEntry,
			AirbusEventListener: event.AirbusEventListener,
			ConcurrentListeners: 10,
		}
		errorClientConfig := make(map[string]interface{})
		for key, value := range this.clientConfig {
			switch key {
			case offsetKey:
				errorClientConfig[key] = sarama.OffsetOldest
			default:
				errorClientConfig[key] = value
			}
		}
		err := this.initializeBCPListener(&errorEntity, errorClientConfig, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// instantiating single/multiple listeners based on bcp logic
func (this *consumer) initializeBCPListener(entity *entry.EventListenerEntity, clientConfig map[string]interface{}, shouldExecute bool) error {
	// fetches the topics the consumer should listen to
	topics, err := getTopics(this.serviceUrl, this.appName, entity.AirbusEvent.AppName, entity.AirbusEvent.EventName)
	if err != nil {
		return err
	}
	// for the topics fetched, instantiate new listener for each
	for _, topic := range topics {
		err := this.initializeListener(entity, topic, clientConfig, shouldExecute)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *consumer) initializeListener(entity *entry.EventListenerEntity, topic string, clientConfig map[string]interface{}, shouldExecute bool) error {
	var groupId string
	config := sarama.NewConfig()
	for key, value := range clientConfig {
		if value != nil {
			switch value.(type) {
			case string:
				switch key {
				case serverKey:
					this.hosts = strings.Split(value.(string), ",")
				case clientKey:
					config.ClientID = value.(string)
				case offsetKey:
					if offsetReset, err := strconv.ParseInt(value.(string), 10, 64); err != nil {
						return err
					} else {
						config.Consumer.Offsets.Initial = offsetReset
					}
				case commitIntervalKey:
					if commitInterval, err := strconv.ParseInt(value.(string), 10, 64); err != nil {
						return err
					} else {
						config.Consumer.Offsets.CommitInterval = time.Duration(commitInterval) * time.Second
					}
				case groupKey:
					groupId = value.(string)
				case schemaRegistryKey:
					this.schemaRegistry = strings.Split(value.(string), ",")
				case concurrencyKey:
					if entity.ConcurrentListeners == 0 {
						if concurrency, err := strconv.ParseInt(value.(string), 10, 64); err != nil {
							return err
						} else {
							entity.ConcurrentListeners = int(concurrency)
						}
					}
				}

			case int:
				switch key {
				case offsetKey:
					config.Consumer.Offsets.Initial = int64(value.(int))
				case commitIntervalKey:
					config.Consumer.Offsets.CommitInterval = time.Duration(value.(int)) * time.Second
				case concurrencyKey:
					if entity.ConcurrentListeners == 0 {
						entity.ConcurrentListeners = value.(int)
					}
				}

			case int64:
				switch key {
				case offsetKey:
					config.Consumer.Offsets.Initial = value.(int64)
				}
			}
		}
	}
	config.Consumer.Return.Errors = true
	config.Version = constants.KafkaVersion
	if config.ClientID == constants.DefaultClientId {
		config.ClientID = this.appName + "-" + topic
	}

	// Default fallbacks for Airbus Event Listeners
	if entity.AirbusEventListener == nil {
		entity.AirbusEventListener = entry.AirbusDefaultListener()
	}
	if entity.AirbusHeadersEventListener == nil {
		entity.AirbusHeadersEventListener = entry.AirbusDefaultHeaderListener()
	}
	if entity.AirbusCompleteEventListener == nil {
		entity.AirbusCompleteEventListener = entry.AirbusDefaultCompleteListener()
	}

	exec := executor.NewListenerExecutor(this.hosts, this.schemaRegistry, groupId, this.serviceUrl, this.appName, topic,
		config, this.statsD, this.errorQProducer, this.wg, entity.ConsumerCount)

	consumerGroupID, err := util.GetConsumerGroupName(this.appName, entity.AirbusEvent)
	if err != nil {
		return err
	}
	this.metadataCache.PutConsumerCacheEntry(consumerGroupID, &entry.ConsumerCacheEntry{
		Entity:       entity,
		Executor:     exec,
		IsSubscribed: shouldExecute,
	})

	if shouldExecute {
		err := exec.Execute(entity)
		if err != nil {
			return err
		}
	}
	return nil
}

// consumer bcp compliant logic goes in here. depending on app name, event name and the service url
// decision is made on which topics the consumer should listen to. For more details look into the
// unit test of this method.
func getTopics(serviceUrl, consumerAppName, producerAppName, eventName string) ([]string, error) {
	var topics []string
	topicWithoutPrefix, _ := util.GetTopicName("", producerAppName, eventName) // fetching topic name with out any prefixes
	// For service/meta related topics, the sdk should listen from all the topics
	if producerAppName == constants.ConsumerRegisterationAppName {
		topics, err := util.GetAllDCTopicName(serviceUrl, producerAppName, eventName)
		if err != nil {
			return topics, err
		}
		topics = append(topics, topicWithoutPrefix)
		return topics, nil
	}
	// fetching the consumer details for the app name
	appEntries, err := util.GetConsumerEntity(serviceUrl, consumerAppName)
	if err != nil {
		return topics, err
	}
	if len(appEntries) == 0 {
		return topics, errors.New(constants.ErrorAppNameNotFound)
	}
	// from all the entries of consumer details for the given app, finding out the current consumer details from
	// event name
	appEntry := appEntries[0]
	var consumerEntry entry.ConsumerEventEntry
	for _, value := range appEntry.Consumes {
		if value.AppName == producerAppName && value.EventName == eventName {
			consumerEntry = value
			break
		}
	}
	// logic goes here for normal clients
	if consumerEntry.IsBCPCompliant {
		switch consumerEntry.ConsumerType {
		case entry.GlobalConsumer:
			// if global consumer, the consumer should listen to all DCs
			allDCTopics, err := util.GetAllDCTopicName(serviceUrl, producerAppName, eventName)
			if err != nil {
				return topics, err
			}
			for _, value := range allDCTopics {
				topics = append(topics, value)
			}
		case entry.LocalConsumer:
			// if local consumer, the consumer should listen to the DC it is in, can be fetched from service url
			topic, err := util.GetTopicName(serviceUrl, producerAppName, eventName)
			if err != nil {
				return topics, err
			}
			topics = append(topics, topic)
		}
	} else { // if not bcp compliant the consumer should listen to old topic i.e topic without prefix
		topics = append(topics, topicWithoutPrefix)
	}
	return topics, nil
}
