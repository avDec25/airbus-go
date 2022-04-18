package entry

type EventListener interface {
	OnEvent(key string, value interface{}) error
}

type airbusDefaultListener struct{}

func AirbusDefaultListener() EventListener {
	return &airbusDefaultListener{}
}

func (this *airbusDefaultListener) OnEvent(key string, value interface{}) error {
	log.Debugf("No Listeners attached to event by client, calling default listener %+v", value)
	return nil
}
