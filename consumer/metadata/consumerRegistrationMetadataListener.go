package metadata

import (
	"bitbucket.mynt.myntra.com/plt/airbus-go/constants"
	"bitbucket.mynt.myntra.com/plt/airbus-go/entry"
	"bitbucket.mynt.myntra.com/plt/airbus-go/util"
	"encoding/json"
	"errors"
	"sync"
)

type consumerRegistrationMetadata struct {
	appName       string
	metadataCache entry.ConsumerMetadataCache
}

func NewConsumerRegistrationMetadataListener(appName string, metadataCache entry.ConsumerMetadataCache) entry.EventListener {
	return &consumerRegistrationMetadata{
		appName:       appName,
		metadataCache: metadataCache,
	}
}

func (this *consumerRegistrationMetadata) OnEvent(key string, value interface{}) error {
	var consumerRegEvent entry.RegistrationEvent
	var err error

	switch value.(type) {
	case string:
		err = json.Unmarshal([]byte(value.(string)), &consumerRegEvent)
	case []byte:
		err = json.Unmarshal(value.([]byte), &consumerRegEvent)
	}
	if err != nil {
		log.Error(err.Error())
	} else {
		log.Infof("Got Consumer update event : %+v\n", consumerRegEvent)

		if consumerRegEvent.AppName != "" {
			switch consumerRegEvent.Behaviour {
			case entry.Subscribe:
				handleSubscribeEvent(&consumerRegEvent, this.metadataCache)
			case entry.Unsubscribe:
				handleUnsubscribeEvent(&consumerRegEvent, this.metadataCache)
			default:
				log.Debug("Subscribe / Unsubscribe event not identified")
			}
		}
	}
	return nil
}

func handleSubscribeEvent(event *entry.RegistrationEvent, metadataCache entry.ConsumerMetadataCache) {
	eventEntry := event.EventEntry
	consumerGroupID, err := util.GetConsumerGroupName(event.AppName, eventEntry)
	if err != nil {
		log.Errorf(constants.ErrorSubscribeEvent, event, err.Error())
	} else {
		consumerCacheList := metadataCache.GetConsumerCacheEntry(consumerGroupID)
		for _, consumerCacheEntry := range consumerCacheList {
			if consumerCacheEntry != nil {
				if *consumerCacheEntry == (entry.ConsumerCacheEntry{}) {
					err = errors.New(constants.ErrorConsumerCache)
				} else {
					if !consumerCacheEntry.IsSubscribed {
						err = consumerCacheEntry.Executor.Execute(consumerCacheEntry.Entity)
						consumerCacheEntry.IsSubscribed = true
					} else {
						log.Infof("Already in play state for group: %s\n", consumerGroupID)
					}
				}
			}
			if err != nil {
				log.Errorf(constants.ErrorSubscribeEvent, event, err.Error())
			}
		}
	}
}

func handleUnsubscribeEvent(event *entry.RegistrationEvent, metadataCache entry.ConsumerMetadataCache) {
	eventEntry := event.EventEntry
	consumerGroupID, err := util.GetConsumerGroupName(event.AppName, eventEntry)
	if err != nil {
		log.Errorf(constants.ErrorSubscribeEvent, event, err.Error())
	} else {
		consumerCacheList := metadataCache.GetConsumerCacheEntry(consumerGroupID)
		for _, consumerCacheEntry := range consumerCacheList {
			if consumerCacheEntry != nil {
				if *consumerCacheEntry == (entry.ConsumerCacheEntry{}) {
					err = errors.New(constants.ErrorConsumerCache)
				} else {
					if consumerCacheEntry.IsSubscribed {
						consumerCount, wg := 1, sync.WaitGroup{}
						if consumerCacheEntry.Entity.ConsumerCount > 0 {
							consumerCount = consumerCacheEntry.Entity.ConsumerCount
						}
						for i := 0; i < consumerCount; i++ {
							wg.Add(1)
							go func(count int) {
								defer wg.Done()
								consumerCacheEntry.Executor.StopGroup(count)
							}(i)
						}
						wg.Wait()
						consumerCacheEntry.IsSubscribed = false
					} else {
						log.Infof("Already unsubscribed (not listening) for group: %s\n", consumerGroupID)
					}
				}
				if err != nil {
					log.Errorf(constants.ErrorUnsubscribeEvent, event, err.Error())
				}
			}
		}
	}
}
