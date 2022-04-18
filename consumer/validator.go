package consumer

import (
	"bitbucket.mynt.myntra.com/plt/airbus-go/constants"
	"bitbucket.mynt.myntra.com/plt/airbus-go/entry"
	"bitbucket.mynt.myntra.com/plt/airbus-go/util"
	"errors"
	"fmt"
	"time"
)

const sqlTimestampLayout = `2006-01-02T15:04:05Z`

func (this *consumer) isValid() error {
	data, err := util.GetConsumerEntity(this.serviceUrl, this.appName)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return errors.New(fmt.Sprintf(constants.ConsumerNotRegistered, this.appName))
	}
	appEntry := data[0]
	listeners := this.eventListeners

	dcIdentifier, err := util.GetDCPrefix(this.serviceUrl)
	if err != nil {
		return err
	}
	var clientEvents []string
	for _, listener := range listeners {
		topicName, err := util.GetTopicName("", listener.AirbusEvent.AppName, listener.AirbusEvent.EventName)
		if err != nil {
			return err
		}
		clientEvents = append(clientEvents, topicName)
		consumerGroupName, err := util.GetConsumerGroupName(this.appName, listener.AirbusEvent)
		if err != nil {
			return err
		}
		listener.SubscriberTime = constants.OffsetDefaultValue
		offsetResponse, err := util.GetConsumerOffsetConfig(this.serviceUrl, consumerGroupName)
		if err != nil {
			log.Error("No offset entry found for the consumer group", err.Error())
		} else {
			listener.SubscriberTime = getSubscriberTime(dcIdentifier, offsetResponse)
		}
	}
	var registeredEvents []interface{}
	for _, event := range appEntry.Consumes {
		topicName, err := util.GetTopicName("", event.AppName, event.EventName)
		if err != nil {
			return err
		}
		registeredEvents = append(registeredEvents, topicName)
	}
	for _, event := range clientEvents {
		if !util.Contains(event, registeredEvents) {
			return errors.New(fmt.Sprintf(constants.ConsumerEventNotRegistered, this.appName, event))
		}
	}
	return nil
}

// get offset value from offset entries that is not of the current dc
func getSubscriberTime(dcIdentifier string, offsetEntries []entry.OffsetEntry) (subscriberTime int64) {
	subscriberTime = constants.OffsetDefaultValue
	for _, value := range offsetEntries {
		if value.DCPrefix != dcIdentifier {
			t, err := time.Parse(sqlTimestampLayout, value.Timestamp)
			if err != nil {
				log.Error(err.Error())
			}
			fmt.Println()
			subscriberTime = t.UnixNano() / 1000000
			break
		}
	}
	return
}
