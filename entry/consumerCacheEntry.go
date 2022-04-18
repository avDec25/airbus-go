package entry

type ConsumerCacheEntry struct {
	Executor     ListenerExecutor
	Entity       *EventListenerEntity
	IsSubscribed bool
}
