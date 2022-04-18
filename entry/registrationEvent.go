package entry

import "time"

type RegistrationEvent struct {
	AppName    string      `json:"appName"`
	EventEntry *EventEntry `json:"eventName"`
	Timestamp  time.Time   `json:"timestamp"`
	Behaviour  Status      `json:"behaviour"`
}
