package producer

import "fmt"

func (this *producer) shutdown() {
	// Idempotent closing
	defer func() {
		if e := recover(); e != nil {
			fmt.Println(e)
		}
	}()

	GetProducerFactoryInstance().clean()
}
