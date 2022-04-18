package entry

type ConsumerMetadataCache interface {
	GetConsumerCacheEntry(topic string) []*ConsumerCacheEntry
	PutConsumerCacheEntry(topic string, newConsumerCacheEntry *ConsumerCacheEntry)
	CleanAirbusConsumers()
	IncrementConsumerCount(consumerGroupID string, concurrency int)
	DecrementConsumerCount(consumerGroupID string, concurrency int)
	GetConsumerCacheCount(consumerGroupID string) int
}
