package cluster

import (
	"bitbucket.mynt.myntra.com/plt/airbus-go/constants"
	"bitbucket.mynt.myntra.com/plt/airbus-go/entry"
	"bitbucket.mynt.myntra.com/plt/airbus-go/logger"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var log = logger.GetLogger()

type AirbusConsumer struct {
	AppName        string
	ServiceUrl     string
	Config         map[string]interface{}
	EventListeners []*entry.EventListenerEntity
}

func GetAirbusConsumer(hosts, topics []string, consumerGroup string, conf *sarama.Config) *cluster.Consumer {
	sigs := make(chan os.Signal, 1)
	var done bool
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		done = true
	}()

RECONNECT:
	consumer, err := cluster.NewConsumer(hosts, consumerGroup, topics, getClusterConfig(conf))
	if err != nil && !done {
		log.Errorf("Smart Kafka consumer error: %s\n", err)
		time.Sleep(constants.BackoffTime)
		goto RECONNECT
	} else if done {
		return nil
	}

	log.Info("Smart Kafka consumer connnected...")
	return consumer
}

func getClusterConfig(conf *sarama.Config) *cluster.Config {
	clusterConfig := cluster.NewConfig()
	clusterConfig.Config = *conf
	clusterConfig.Group.Return.Notifications = true
	return clusterConfig
}
