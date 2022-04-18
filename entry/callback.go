package entry

import (
	"github.com/Shopify/sarama"
	"time"
)

type Result struct {
	Topic     string
	Key       sarama.Encoder
	Value     sarama.Encoder
	Timestamp time.Time
	Offset    int64
	Partition int32
}

type ListenableCallback interface {
	OnSuccess(result *Result)
	OnFailure(err error, msg *Result)
}
