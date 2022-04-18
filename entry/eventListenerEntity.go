package entry

type EventListenerEntity struct {
	AirbusEvent                 *EventEntry
	AirbusEventListener         EventListener
	AirbusHeadersEventListener  EventListenerHeaders
	AirbusCompleteEventListener EventListenerComplete
	ConcurrentListeners         int
	AutoCommitDisable           bool
	ConsumerCount               int
	SubscriberTime              int64
}
