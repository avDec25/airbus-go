package entry

type ListenerExecutor interface {
	Execute(entity *EventListenerEntity) error
	StopGroup(count int)
}
