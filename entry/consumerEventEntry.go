package entry

type ConsumerEventEntry struct {
	EventId        float64
	Concurrency    float64
	IsSubscribed   bool
	AppName        string
	EventName      string
	IsBCPCompliant bool
	ConsumerType   string
}

func NewConsumerEventEntry() *ConsumerEventEntry {
	return &ConsumerEventEntry{
		Concurrency:    1,
		IsSubscribed:   true,
		IsBCPCompliant: true,
		ConsumerType:   LocalConsumer,
	}
}
