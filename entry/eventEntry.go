package entry

type EventEntry struct {
	AppName      string
	EventName    string
	PartitionKey string
	Data         interface{}
	Headers      map[string]string
}
