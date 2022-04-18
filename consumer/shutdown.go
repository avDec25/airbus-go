package consumer

import "fmt"

func (this *consumer) shutdown() {
	// Idempotent closing
	defer func() {
		if e := recover(); e != nil {
			fmt.Println(e)
		}
	}()

	this.wg.Wait()
	this.metadataCache.CleanAirbusConsumers()
	if this.statsD != nil {
		this.statsD.Close()
	}
	this.cShutdown <- true
}
