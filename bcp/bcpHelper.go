package bcp

import (
	"github.com/Shopify/sarama"
	"github.com/avDec25/airbus-go/constants"
	"github.com/avDec25/airbus-go/entry"
	"github.com/avDec25/airbus-go/logger"
	"github.com/avDec25/airbus-go/util"
	"strings"
)

const (
	serverKey = "bootstrap.servers"
)

var log = logger.GetLogger()

func GetCheckpointForTimestamp(serviceUrl string, event entry.EventEntry, consumerGroupName string, subscriberTime int64) (offsetsMap map[int32]int64, err error) {
	offsetResponse, err := util.GetConsumerOffsetConfig(serviceUrl, consumerGroupName)
	if err != nil {
		return
	}
	invertDCAppender, err := getOtherDCIdentifier(offsetResponse, serviceUrl)
	if err != nil {
		return
	}
	topicWithoutPrefix, _ := util.GetTopicName("", event.AppName, event.EventName)
	topicDetails, err := util.GetTopicDetailsCache(serviceUrl, topicWithoutPrefix)
	if err != nil {
		return
	}
	topic := invertDCAppender + "." + topicWithoutPrefix
	if topicDetails.IsBCPCompliant && event.AppName != constants.ConsumerRegisterationAppName && !strings.Contains(event.EventName, "regis") {
		offsetsMap, err = getOffset(serviceUrl, topic, subscriberTime)
	}
	return
}

func getOtherDCIdentifier(offsetEntries []entry.OffsetEntry, serviceUrl string) (string, error) {
	otherDCIdentifier := ""
	dcIdentifier, err := util.GetDCPrefix(serviceUrl)
	if err != nil {
		return otherDCIdentifier, err
	}
	for _, value := range offsetEntries {
		if value.DCPrefix != dcIdentifier {
			otherDCIdentifier = value.DCPrefix
			break
		}
	}
	return otherDCIdentifier, nil
}

func getOffset(serviceUrl, topic string, subscriberTime int64) (offsetsMap map[int32]int64, err error) {
	offsetsMap = make(map[int32]int64)
	clientConfig := sarama.NewConfig()
	clientConfig.Version = constants.KafkaVersion
	brokers, err := getBrokers(serviceUrl)
	if err != nil {
		return
	}
	client, err := sarama.NewClient(brokers, clientConfig)
	if err != nil {
		return
	}
	defer client.Close()
	partitions, err := client.Partitions(topic)
	if err != nil {
		return
	}
	for _, partition := range partitions {
		partitionOffset, err := client.GetOffset(topic, partition, subscriberTime)
		if err != nil {
			log.Errorf("error getting offset for partition %+d. %+v\n", partition, err.Error())
		} else {
			offsetsMap[partition] = partitionOffset
		}
	}
	log.Infof("offsets for topic %+v is %+v", topic, offsetsMap)
	return offsetsMap, nil
}

func getBrokers(serviceUrl string) ([]string, error) {
	var brokers []string
	consumerConfig, err := util.GetConsumerConfig(serviceUrl)
	if err != nil {
		return brokers, err
	}
	if value, ok := consumerConfig[serverKey]; ok {
		brokers = strings.Split(value.(string), ",")
	}
	return brokers, nil
}
