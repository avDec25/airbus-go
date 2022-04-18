package entry

type TopicEntry struct {
	Id             float64
	EventName      string
	AppName        string
	Schema         string
	SchemaId       int
	SchemaType     SerializationSchema
	Status         Status
	Profile        Profile
	Replication    ReplicationTypeEnum
	IsBCPCompliant bool
	ClusterId      int 
    ClusterIdOld   int
	IsInMigration  bool 
}

func NewTopicEntry(entry map[string]interface{}) *TopicEntry {
	topicEntry := TopicEntry{}

	if entry["id"] != nil {
		topicEntry.Id = entry["id"].(float64)
	}
	if entry["eventName"] != nil {
		topicEntry.EventName = entry["eventName"].(string)
	}
	if entry["appName"] != nil {
		topicEntry.AppName = entry["appName"].(string)
	}
	if entry["schema_id"] != nil {
		topicEntry.SchemaId = int(entry["schema_id"].(float64))
	}
	if entry["schema"] != nil {
		topicEntry.Schema = entry["schema"].(string)
	}
	if entry["schemaType"] != nil {
		topicEntry.SchemaType = SerializationSchema(entry["schemaType"].(string))
	}
	if topicEntry.Status = Subscribe; entry["status"] != nil {
		topicEntry.Status = Status(entry["status"].(string))
	}
	if topicEntry.Profile = Medium; entry["profile"] != nil {
		topicEntry.Profile = Profile(entry["profile"].(string))
	}
	if topicEntry.Replication = Local; entry["replication"] != nil {
		topicEntry.Replication = ReplicationTypeEnum.valueOf(Local, entry["replication"].(string))
	}
	if entry["clusterId"] != nil {
		topicEntry.ClusterId = int(entry["clusterId"].(float64))
	}
	if entry["isInMigration"] != nil {
		topicEntry.IsInMigration = entry["isInMigration"].(bool)
	}
	if entry["appName"] != nil {
		topicEntry.AppName = entry["appName"].(string)
	}
	if topicEntry.IsBCPCompliant = false; entry["isBCPCompliant"] != nil {
		topicEntry.IsBCPCompliant = entry["isBCPCompliant"].(bool)
	}
	return &topicEntry
}
