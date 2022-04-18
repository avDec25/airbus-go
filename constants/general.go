package constants

import (
	"github.com/Shopify/sarama"
	"math"
	"time"
)

const (
	// Consumer Configs
	ErrorQueueAppender             = "_error"
	ConsumerGroupAppender          = "consumer-group"
	ConsumerGroup                  = "group.id"
	ConsumerRegisterationAppName   = "airbus"
	ConsumerRegisterationEventName = "registeration"
	ConsumerConfigPrefix           = "/bus/v1/config/goConsumer"
	ConsumerEntityPrefix           = "/bus/v1/app/"
	DefaultClientId                = "sarama"

	BackoffTime = 5 * time.Second

	// Topic Config
	GetTopicPrefix = "/bus/v1/topic/"

	// Producer Configs
	ProducerConfigPrefix = "/bus/v1/config/goProducer"

	// Statsd Configs
	ProducerStatsPrefix = "airbus.producer"
	ConsumerStatsPrefix = "airbus.consumer"
	StatsdConfigPrefix  = "/bus/v1/config/statsD"
	SuccessAppender     = ".success"
	FailureAppender     = ".failure"
	ErrorAppender       = ".error"
	MsgProcessAppender  = ".msgProcess"

	// Authorization
	Authorization = "Basic WVhCcFlXUnRhVzQ2YlRGOnVkSEpoVWpCamEyVjBNVE1oSXc9PQ=="

	// BCP config
	DCAppenderConfigPrefixPath     = "/bus/v1/config/dcprefix"
	ConsumerOffsetConfigPrefixPath = "/bus/v2/offset/"
	DCRecoveryConfigPath           = "/bus/v1/config/isRecoveryAllowed/"
	DCListConfigPath               = "/bus/v1/config/dcPrefixList/"
	DCConfigPrefix                 = "prefix"

	// Cache Config
	DefaultCacheExpiration = 15 * time.Minute

	// Http Client Config
	Timeout = 5 * time.Second

	// BCP constants
	OffsetDefaultValue = math.MaxInt64

	// Concurrency limit config
	ConcurrencyLimitPath     = "/bus/v1/config/concurrencyLimit/"
	ConcurrencyPrefix        = "concurrency"
	DefautltConcurrencyLimit = 10
)

var KafkaVersion = sarama.V1_1_0_0
