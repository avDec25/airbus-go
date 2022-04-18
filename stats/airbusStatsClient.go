package stats

import (
	"bitbucket.mynt.myntra.com/plt/airbus-go/logger"
	"bitbucket.mynt.myntra.com/plt/airbus-go/constants"
	"bitbucket.mynt.myntra.com/plt/airbus-go/util"
	"fmt"
)

var consumerStatsDClient StatsdCollector
var producerStatsDClient StatsdCollector

func GetProducerStatsdClient(serviceUrl string) StatsdCollector {
	if producerStatsDClient == nil {
		producerStatsDClient = getStatsdClient(serviceUrl, constants.ProducerStatsPrefix)
	}
	return producerStatsDClient
}

func GetConsumerStatsdClient(serviceUrl string) StatsdCollector {
	if consumerStatsDClient == nil {
		consumerStatsDClient = getStatsdClient(serviceUrl, constants.ConsumerStatsPrefix)
	}
	return consumerStatsDClient
}

func getStatsdClient(serviceUrl, prefix string) StatsdCollector {
	config, err := util.GetStatsdConfig(serviceUrl)
	if err != nil {
		log.Error(err.Error())
		return nil
	}

	if config["statsdHost"] == nil || config["statsdPort"] == nil {
		log.Error(constants.HostPortNotFound)
		return nil
	}

	c, err := InitializeStatsdCollector(&StatsdCollectorConfig{
		StatsdAddr: fmt.Sprintf("%s:%s", config["statsdHost"].(string), config["statsdPort"].(string)),
		Prefix:     prefix,
	})
	if err != nil {
		log.Errorf("Could not initialize statsd client: %v", err)
		return c // returning Noop Client
	}
	log=logger.GetLogger()
	log.Info("Connected with Statsd Collector")
	return c
}
