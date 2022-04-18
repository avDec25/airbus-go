package producer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/avDec25/airbus-go/avro"
	"github.com/avDec25/airbus-go/constants"
	"github.com/avDec25/airbus-go/entry"
	goAvro "github.com/elodina/go-avro"
	"github.com/xeipuuv/gojsonschema"
)

func (this *producer) validateAndSend(topic string, topicEntry *entry.TopicEntry, event *entry.EventEntry) (int32, int64, error) {
	if topicEntry.SchemaType == "" {
		return 0, 0, errors.New(constants.ErrorSchemaNotFound)
	}

	var data string
	switch event.Data.(type) {
	case string:
		data = event.Data.(string)
	default:
		if jsonData, err := json.Marshal(event.Data); err != nil {
			log.Error(err.Error())
			return 0, 0, errors.New(constants.ErrorValidatingSchema)
		} else {
			data = string(jsonData[:])
		}
	}
	eventData := gojsonschema.NewReferenceLoader(data)
	eventSchema := gojsonschema.NewReferenceLoader(topicEntry.Schema)

	report, err := gojsonschema.Validate(eventSchema, eventData)
	if err != nil {
		log.Error(err.Error())
		return 0, 0, errors.New(constants.ErrorValidatingSchema)
	}

	if report.Valid() {
		return this.send(topic, event)
	} else {
		log.Error("Validation Report:\n")
		for _, desc := range report.Errors() {
			log.Errorf("- %s\n", desc)
		}
		return 0, 0, errors.New(constants.InvalidData)
	}
}

func (this *producer) send(topic string, event *entry.EventEntry) (int32, int64, error) {
	switch event.Data.(type) {
	case string:
		var messageKey sarama.Encoder
		if event.PartitionKey != "" {
			messageKey = sarama.ByteEncoder(event.PartitionKey)
		}

		var headers []sarama.RecordHeader
		if len(event.Headers) > 0 {
			for key, value := range event.Headers {
				headers = append(headers, sarama.RecordHeader{
					Key:   []byte(key),
					Value: []byte(value),
				})
			}
		}
		message := &sarama.ProducerMessage{
			Topic:   topic,
			Key:     messageKey,
			Value:   sarama.ByteEncoder(event.Data.(string)),
			Headers: headers,
		}
		saramaProducer, err := GetProducerFactoryInstance().getProducer(event.AppName, event.EventName, this.sync, this.config, this.clientProvidedAirbusConfig.ClientCallback, this.serviceUrl)
		if err != nil {
			return 0, 0, fmt.Errorf("Error getting producer from factory: %s", err)
		}
		return saramaProducer.Send(message)
	default:
		return 0, 0, errors.New(constants.ErrPlainEventDataError)
	}
}

func getField(key string, fields []*goAvro.SchemaField) goAvro.Schema {
	for _, field := range fields {
		if field.Name == key {
			return field.Type.(*goAvro.ArraySchema).Items
		}
	}
	return nil
}

func createGenericRecord(schema goAvro.Schema, data map[string]interface{}) *goAvro.GenericRecord {
	if schema != nil {
		record := goAvro.NewGenericRecord(schema)
		fields := schema.(*goAvro.RecordSchema).Fields
		for key, value := range data {
			switch value.(type) {
			case []map[string]interface{}:
				recordArray := []*goAvro.GenericRecord{}
				for _, recordValue := range value.([]map[string]interface{}) {
					recordArray = append(recordArray, createGenericRecord(getField(key, fields), recordValue))
				}
				record.Set(key, recordArray)
			case map[string]interface{}:
				record.Set(key, createGenericRecord(getField(key, fields), value.(map[string]interface{})))
			default:
				record.Set(key, value)
			}
		}
		return record
	}
	return nil
}

func (this *producer) sendAvro(topic string, topicEntry *entry.TopicEntry, event *entry.EventEntry) (int32, int64, error) {
	var record interface{}
	schema, err := goAvro.ParseSchema(topicEntry.Schema)
	if err != nil {
		return 0, 0, err
	}
	switch schema.Type() {
	case goAvro.Record:
		record = createGenericRecord(schema, event.Data.(map[string]interface{}))
		if record == nil {
			return 0, 0, errors.New(constants.InvalidSchemaError)
		}
	default:
		record = event.Data
	}

	var value []byte
	url := this.schemaRegistry
	for i := 0; i < len(url); i++ {
		if value, err = avro.NewKafkaAvroEncoder(url[i]).Encode(record); err == nil {
			var messageKey sarama.Encoder
			if event.PartitionKey != "" {
				messageKey = sarama.ByteEncoder(event.PartitionKey)
			}

			var headers []sarama.RecordHeader
			if len(event.Headers) > 0 {
				for key, value := range event.Headers {
					headers = append(headers, sarama.RecordHeader{
						Key:   []byte(key),
						Value: []byte(value),
					})
				}
			}
			message := &sarama.ProducerMessage{
				Topic:   topic,
				Key:     messageKey,
				Value:   sarama.ByteEncoder(value),
				Headers: headers,
			}
			saramaProducer, err := GetProducerFactoryInstance().getProducer(event.AppName, event.EventName, this.sync, this.config, this.clientProvidedAirbusConfig.ClientCallback, this.serviceUrl)
			if err != nil {
				return 0, 0, fmt.Errorf("Error getting producer from Factory: %s", err)
			}
			return saramaProducer.Send(message)
		}
	}
	return 0, 0, err
}
