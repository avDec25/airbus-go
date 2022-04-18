package entry

type EventListenerHeaders interface {
	OnEvent(key string, value interface{}, headers map[string]string) error
}

type airbusDefaultHeaderListener struct{}

func AirbusDefaultHeaderListener() EventListenerHeaders {
	return &airbusDefaultHeaderListener{}
}

func (this *airbusDefaultHeaderListener) OnEvent(key string, value interface{}, headers map[string]string) error {
	log.Debugf("No Listeners attached to event by client, calling default header listener %+v", value)
	return nil
}
