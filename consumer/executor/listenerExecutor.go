package executor

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/avDec25/airbus-go/bcp"
	"github.com/avDec25/airbus-go/constants"
	"github.com/avDec25/airbus-go/consumer/cluster"
	"github.com/avDec25/airbus-go/consumer/listener"
	"github.com/avDec25/airbus-go/consumer/metadata"
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/logger"
	"github.com/avDec25/airbus-go/stats"
	"github.com/avDec25/airbus-go/util"
	saramaCluster "github.com/bsm/sarama-cluster"
	"strings"
	"sync"
	"time"
)

const rebalanceOk = `rebalance OK`

var log = logger.GetLogger()

type listenerExecutor struct {
	hosts           []string
	schemaRegistry  []string
	topic           string
	groupId         string
	serviceUrl      string
	appName         string
	numberOfRetries int
	config          *sarama.Config
	statsD          stats.StatsdCollector
	errorQProducer  entry.ErrorQProducer
	airbusConsumer  []*saramaCluster.Consumer
	offsetStash     []*saramaCluster.OffsetStash
	close           chan bool
	wg              sync.WaitGroup
	consumerCount   int
	once            bool
	consumerGroupID string
}

func NewListenerExecutor(hosts, schemaRegistry []string, groupId, serviceUrl, appName, topic string, config *sarama.Config,
	statsD stats.StatsdCollector, errorQProducer entry.ErrorQProducer, wg sync.WaitGroup, count int) entry.ListenerExecutor {

	if count == 0 {
		count = 1
	}

	return &listenerExecutor{
		hosts:           hosts,
		schemaRegistry:  schemaRegistry,
		groupId:         groupId,
		serviceUrl:      serviceUrl,
		appName:         appName,
		topic:           topic,
		config:          config,
		numberOfRetries: 3,
		statsD:          statsD,
		errorQProducer:  errorQProducer,
		wg:              wg,
		consumerCount:   count,
		airbusConsumer:  make([]*saramaCluster.Consumer, count),
		offsetStash:     make([]*saramaCluster.OffsetStash, count),
		close:           make(chan bool, 2*count),
		once:            true,
	}
}

func (this *listenerExecutor) Execute(entity *entry.EventListenerEntity) error {
	this.close = make(chan bool, 2*this.consumerCount)
	this.consumerGroupID, _ = util.GetConsumerGroupName(this.appName, entity.AirbusEvent)
	concurrencyLimit, err := util.GetConcurrencyLimit(this.serviceUrl)
	if err != nil {
		return err
	}
	if metadata.GetConsumerCacheInstance().GetConsumerCacheCount(this.consumerGroupID+this.topic)+this.consumerCount > concurrencyLimit {
		return errors.New("breached the maximum allowed consumer concurrency for a topic " + this.topic)
	}
	metadata.GetConsumerCacheInstance().IncrementConsumerCount(this.consumerGroupID+this.topic, this.consumerCount)
	log.Infof("Consumer Config: %+v\n", this.config.Consumer)

	if this.groupId == "" {
		groupId, err := util.GetConsumerGroupName(this.appName, entity.AirbusEvent)
		if err != nil {
			return err
		}
		this.groupId = groupId
	}

	topicWithoutPrefix, _ := util.GetTopicName("", entity.AirbusEvent.AppName, entity.AirbusEvent.EventName)
	topicEntry, err := util.GetTopicDetails(this.serviceUrl, topicWithoutPrefix)
	if err != nil {
		return err
	}

	hosts, err := this.getHostList(entity.AirbusEvent.AppName, entity.AirbusEvent.EventName, this.serviceUrl)
	for i := 0; i < this.consumerCount; i++ {
		this.offsetStash[i] = saramaCluster.NewOffsetStash()
		this.airbusConsumer[i] = cluster.GetAirbusConsumer(hosts, []string{this.topic}, this.groupId, this.config)
		if this.airbusConsumer[i] == nil {
			return errors.New(constants.NilConsumer)
		}
		go this.startConsumer(entity, topicEntry.SchemaType, i, this.topic)
	}
	return nil
}

func (this *listenerExecutor) startConsumer(entity *entry.EventListenerEntity, schemaType entry.SerializationSchema, count int, topic string) {
	log.Infof("Starting consumer for topic %+v\n", topic)
	if entity.ConcurrentListeners == 0 {
		entity.ConcurrentListeners = 1
	}
	consumerSem := make(entry.Semaphore, entity.ConcurrentListeners)

	// Logs all errors
	go func(c int) {
		for err := range this.airbusConsumer[c].Errors() {
			log.Errorf("AirbusConsumerError: %s\n", err.Error())
		}
	}(count)

	// Logs all re-balancing notifications
	go func(c int) {
		for notf := range this.airbusConsumer[c].Notifications() {
			log.Infof("AirbusConsumerNotif Rebalanced: %+v\n", notf)
			// after rebalance is triggered, check whether the rebalance is triggered for the topic that requires offset
			// migration. If required reset offsets and commit them. Then again trigger a rebalance.
			notfMsg := fmt.Sprintf("%v", notf)
			if this.once && strings.Contains(notfMsg, topic) && strings.Contains(notfMsg, rebalanceOk) {
				this.checkAndResetOffsets(entity, topic)
				this.once = false    // making it false so that this is not triggered again
				this.restart(entity) // restart to trigger a rebalance
			}
		}
	}(count)

	this.once = this.isOnceRequired(entity, topic) // assigning whether the topic requires a offset reset migration
	if this.once {
		log.Infof("Resetting offset required for topic %+v . After the reset the consumer will be restarted", topic)
	}

	if !this.once { //if not required only then start consumer processing thread
		log.Infof("No resetting offset required for topic %+v", topic)
		switch entity.AutoCommitDisable {
		case false:
			for rawMsg := range this.airbusConsumer[count].Messages() {
				consumerSem.Lock()
				this.wg.Add(1)
				go func(msg *sarama.ConsumerMessage) {
					defer consumerSem.Unlock()
					defer this.wg.Done()

					msgListener := listener.NewMessageListener(entity.AirbusEventListener, entity.AirbusHeadersEventListener,
						entity.AirbusCompleteEventListener, entity.AirbusEvent, this.serviceUrl, this.groupId, this.appName, this.schemaRegistry,
						this.numberOfRetries, this.statsD, this.errorQProducer)
					msgListener.ProcessMessage(msg, schemaType)
					this.airbusConsumer[count].MarkOffset(msg, "consumed")
				}(rawMsg)
			}
		case true:
			go func(c int) {
			CommitLoop:
				for {
					select {
					case <-this.close:
						break CommitLoop
					default:
						if len(this.offsetStash[c].Offsets()) > 0 {
							this.airbusConsumer[c].MarkOffsets(this.offsetStash[c])
							this.airbusConsumer[c].CommitOffsets()
						}
					}
					time.Sleep(this.config.Consumer.Offsets.CommitInterval)
				}
			}(count)

		ConsumerLoop:
			for {
				var localWg sync.WaitGroup
				for i := 0; i < entity.ConcurrentListeners; i++ {
					select {
					case <-this.close:
						break ConsumerLoop
					case rawMsg := <-this.airbusConsumer[count].Messages():
						this.wg.Add(1)
						localWg.Add(1)
						go func(msg *sarama.ConsumerMessage, c int) {
							defer this.wg.Done()
							defer localWg.Done()

							msgListener := listener.NewMessageListener(entity.AirbusEventListener, entity.AirbusHeadersEventListener,
								entity.AirbusCompleteEventListener, entity.AirbusEvent, this.serviceUrl, this.groupId,
								this.appName, this.schemaRegistry, this.numberOfRetries, this.statsD, this.errorQProducer)
							msgListener.ProcessMessage(msg, schemaType)
							this.offsetStash[c].MarkOffset(msg, "consumed")
						}(rawMsg, count)
					}
				}
				localWg.Wait()
				this.airbusConsumer[count].MarkOffsets(this.offsetStash[count])
				this.airbusConsumer[count].CommitOffsets()
			}
		}
	}
}

// restarts the current listener executor by stopping the groups in it and executing again
// used for triggering a rebalance of partitions
func (this *listenerExecutor) restart(entity *entry.EventListenerEntity) {
	for i := 0; i < this.consumerCount; i++ {
		this.StopGroup(i)
	}
	this.Execute(entity)
}

func (this *listenerExecutor) StopGroup(count int) {
	// To close time commit and consumer if consumer was a sync consumer
	for i := 0; i < 2; i++ {
		this.close <- true
	}
	if len(this.offsetStash) > 0 && this.offsetStash[count] != (&saramaCluster.OffsetStash{}) && len(this.offsetStash[count].Offsets()) > 0 {
		this.airbusConsumer[count].MarkOffsets(this.offsetStash[count])
	}
	if len(this.airbusConsumer) > 0 && this.airbusConsumer[count] != (&saramaCluster.Consumer{}) {
		this.airbusConsumer[count].Close()
	}
	metadata.GetConsumerCacheInstance().DecrementConsumerCount(this.consumerGroupID+this.topic, this.consumerCount)
	log.Infof("Stopping consumer %d for topic: %s\n", count+1, this.topic)
}

func (this *listenerExecutor) checkAndResetOffsets(entity *entry.EventListenerEntity, topic string) {
	dcPrefix, err := util.GetDCPrefix(this.serviceUrl)
	if err != nil {
		log.Error(err.Error())
	} else if entity.SubscriberTime > 0 && entity.SubscriberTime != constants.OffsetDefaultValue && !strings.HasPrefix(topic, dcPrefix) {
		log.Info("Offset timestamp found from other dc")
		consumerGroupName, err := util.GetConsumerGroupName(this.appName, entity.AirbusEvent)
		if err != nil {
			log.Error(err.Error())
		} else {
			offsetsMap, err := bcp.GetCheckpointForTimestamp(this.serviceUrl, *entity.AirbusEvent,
				consumerGroupName, entity.SubscriberTime) // for given timestamp fetching the offsets - partitions map
			if err != nil {
				log.Error(err.Error())
			} else {
				for _, consumer := range this.airbusConsumer {
					for partition, offsetToReset := range offsetsMap {
						// for each entry in the map if offset is valid then resetting the offset
						if offsetToReset >= 0 && offsetToReset != constants.OffsetDefaultValue {
							log.Infof("Resetting offset for partition %+v in topic %+v to offset %+v\n", partition, topic, offsetToReset)
							consumer.ResetPartitionOffset(topic, partition, offsetToReset, "reset")
						}
					}
					err = consumer.CommitOffsets() // committing all offsets at once
					if err != nil {
						log.Error(err.Error())
					}
				}
			}
		}
	}
}

// checks whether offset migration is required
func (this *listenerExecutor) isOnceRequired(entity *entry.EventListenerEntity, topic string) bool {
	if this.once {
		dcPrefix, err := util.GetDCPrefix(this.serviceUrl)
		if err != nil {
			log.Error(err.Error())
		} else if entity.SubscriberTime > 0 && entity.SubscriberTime != constants.OffsetDefaultValue && !strings.HasPrefix(topic, dcPrefix) {
			return true
		}
	}
	return false
}

func (this *listenerExecutor) getHostList(appName string, eventName string, serviceUrl string) ([]string, error) {
	topicNameWithoutPrefix, err := util.GetTopicName("", appName, eventName)
	if err != nil {
		return nil, fmt.Errorf("Error getting topicName: %s", err)
	}

	topicEntry, err := util.GetTopicDetails(serviceUrl, topicNameWithoutPrefix)
	if err != nil {
		return nil, fmt.Errorf("Error getting GetTopicDetailsCache: %s", err)
	}
	clusterId := topicEntry.ClusterId
	if topicEntry.IsInMigration && util.IsOffsetInMigration(serviceUrl, this.groupId) {
		clusterId = topicEntry.ClusterIdOld
	}
	hostslist, err := util.GetBootstrapServers(serviceUrl, clusterId)
	if err != nil {
		return nil, fmt.Errorf("Error getting GetBootstrapServers: %s", err)
	}
	return strings.Split(hostslist, ","), nil
}
