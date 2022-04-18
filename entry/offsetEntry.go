package entry

type OffsetEntry struct {
	Id                string  `json:"id"`
	ConsumerGroupName string  `json:"consumerGroupName"`
	DCPrefix          string  `json:"dcprefix"`
	Timestamp         string  `json:"timestamp"`
	Offset            float64 `json:"offset"`
}

func NewOffsetEntry(entry map[string]interface{}) *OffsetEntry {
	offsetEntry := OffsetEntry{}
	if entry["id"] != nil {
		offsetEntry.Id = entry["id"].(string)
	}
	if entry["consumerGroupName"] != nil {
		offsetEntry.ConsumerGroupName = entry["consumerGroupName"].(string)
	}
	if entry["dcprefix"] != nil {
		offsetEntry.DCPrefix = entry["dcprefix"].(string)
	}
	if entry["timestamp"] != nil {
		offsetEntry.Timestamp = entry["timestamp"].(string)
	}
	if entry["offset"] != nil {
		offsetEntry.Offset = entry["offset"].(float64)
	}
	return &offsetEntry
}
