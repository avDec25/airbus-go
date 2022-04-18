package metadata

import (
	"github.com/avDec25/airbus-go/entry"
	"sync"
)

type consumerCache struct {
	metadataCache      map[string][]*entry.ConsumerCacheEntry
	consumerCountCache map[string]int
	lock               sync.RWMutex // mutex needed as reading and writing to cache
}

var instance *consumerCache
var once sync.Once

func GetConsumerCacheInstance() entry.ConsumerMetadataCache {
	once.Do(func() {
		log.Info("Spawned a new consumer metadata cache entry")
		instance = &consumerCache{
			metadataCache:      make(map[string][]*entry.ConsumerCacheEntry),
			consumerCountCache: make(map[string]int),
		}
	})
	return instance
}

func (this *consumerCache) GetConsumerCacheEntry(consumerGroupID string) []*entry.ConsumerCacheEntry {
	this.lock.RLock()
	if consumerCacheEntry, exists := this.metadataCache[consumerGroupID]; exists {
		this.lock.RUnlock()
		return consumerCacheEntry
	}
	this.lock.RUnlock()
	return nil
}

func (this *consumerCache) PutConsumerCacheEntry(consumerGroupID string, newConsumerCacheEntry *entry.ConsumerCacheEntry) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.metadataCache[consumerGroupID] = append(this.metadataCache[consumerGroupID], newConsumerCacheEntry)
}

func (this *consumerCache) CleanAirbusConsumers() {
	this.lock.RLock()
	defer this.lock.RUnlock()

	for _, cache := range this.metadataCache {
		for _, cacheEntry := range cache {
			if cacheEntry.IsSubscribed {
				consumerCount, wg := 1, sync.WaitGroup{}
				if cacheEntry.Entity.ConsumerCount > 0 {
					consumerCount = cacheEntry.Entity.ConsumerCount
				}
				for i := 0; i < consumerCount; i++ {
					wg.Add(1)
					go func(count int) {
						defer wg.Done()
						cacheEntry.Executor.StopGroup(count)
					}(i)
				}
				wg.Wait()
			}
		}
	}
}

func (this *consumerCache) IncrementConsumerCount(consumerGroupID string, concurrency int) {
	this.lock.Lock()
	defer this.lock.Unlock()

	prevCount, _ := this.consumerCountCache[consumerGroupID]
	this.consumerCountCache[consumerGroupID] = prevCount + concurrency
}

func (this *consumerCache) DecrementConsumerCount(consumerGroupID string, concurrency int) {
	this.lock.Lock()
	defer this.lock.Unlock()
	prevCount, _ := this.consumerCountCache[consumerGroupID]
	val := prevCount - concurrency
	if val < 0 {
		this.consumerCountCache[consumerGroupID] = 0
	} else {
		this.consumerCountCache[consumerGroupID] = val
	}
}

func (this *consumerCache) GetConsumerCacheCount(consumerGroupID string) int {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.consumerCountCache[consumerGroupID]
}
