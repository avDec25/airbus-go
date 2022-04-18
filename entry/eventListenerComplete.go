package entry

import "time"

type EventListenerComplete interface {
	OnEvent(key string, value interface{}, topic string, partition int32, offset int64, timestamp,
		blockTimestamp time.Time, headers map[string]string) error
}

type airbusDefaultCompleteListener struct{}

func AirbusDefaultCompleteListener() EventListenerComplete {
	return &airbusDefaultCompleteListener{}
}

func (this *airbusDefaultCompleteListener) OnEvent(key string, value interface{}, topic string, partition int32, offset int64, timestamp,
	blockTimestamp time.Time, headers map[string]string) error {
	log.Debugf("No Listeners attached to event by client, calling default complete listener %+v", value)
	return nil
}
