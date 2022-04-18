package entry

import "github.com/Shopify/sarama"

type MessageListener interface {
	ProcessMessage(msg *sarama.ConsumerMessage, schemaType SerializationSchema)
}
