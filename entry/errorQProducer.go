package entry

type ErrorQProducer interface {
	Publish(errorEvent *EventEntry, originalEventName string)
}
