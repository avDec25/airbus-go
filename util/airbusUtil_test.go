package util

import (
	"github.com/avDec25/airbus-go/constants"
	"github.com/avDec25/airbus-go/entry"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	serviceUrlDC2     = `http://airbuskafka1:7071`
	expectedPrefixDC2 = `dc2`
	serviceUrlDC1     = `http://localhost:7071`
	expectedPrefixDC1 = `dc1`
	appName           = `automation`
	eventName         = `testPlainEventGlobal`
	consumerAppName   = `automationNew`
)

func TestGetDCPrefix(t *testing.T) {
	positiveTests := []struct {
		desc           string
		serviceUrl     string
		expectedPrefix string
	}{
		{
			desc:           "Testing the behavior for getDCPrefix for DC2",
			serviceUrl:     serviceUrlDC2,
			expectedPrefix: expectedPrefixDC2,
		},
		{
			desc:           "Testing the behavior for getDCPrefix for DC1",
			serviceUrl:     serviceUrlDC1,
			expectedPrefix: expectedPrefixDC1,
		},
	}
	// Running all positive tests twice
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualPrefix, err := GetDCPrefix(tt.serviceUrl)
				assert.Equal(t, tt.expectedPrefix, actualPrefix, "Unexpected JSON detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
}

func TestIsRecoverableClient(t *testing.T) {
	positiveTests := []struct {
		desc       string
		serviceUrl string
		expected   bool
	}{
		{
			desc:       "Testing the behavior for isRecoverableClient for DC2",
			serviceUrl: serviceUrlDC2,
			expected:   false,
		},
		{
			desc:       "Testing the behavior for isRecoverableClient for DC1",
			serviceUrl: serviceUrlDC1,
			expected:   false,
		},
	}
	// Running all positive tests twice for regression
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualPrefix, err := IsRecoverableClient(tt.serviceUrl)
				assert.Equal(t, tt.expected, actualPrefix, "Unexpected JSON detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}

}

func TestGetTopicName(t *testing.T) {
	positiveTests := []struct {
		desc       string
		serviceUrl string
		appName    string
		eventName  string
		expected   string
	}{
		{
			desc:       "Testing the behavior with no service url",
			serviceUrl: "",
			appName:    appName,
			eventName:  eventName,
			expected:   "automation-testPlainEventGlobal",
		},
		{
			desc:       "Testing the behavior with service url of DC2",
			serviceUrl: serviceUrlDC2,
			appName:    appName,
			eventName:  eventName,
			expected:   "dc2.automation-testPlainEventGlobal",
		},
	}
	// Running all positive tests twice for regression
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualPrefix, err := GetTopicName(tt.serviceUrl, tt.appName, tt.eventName)
				assert.Equal(t, tt.expected, actualPrefix, "Unexpected topic name detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
}

func TestGetAllDCTopicName(t *testing.T) {
	positiveTests := []struct {
		desc       string
		serviceUrl string
		appName    string
		eventName  string
		expected   []string
	}{
		{
			desc:       "Testing the behavior with service url of DC2",
			serviceUrl: serviceUrlDC2,
			appName:    appName,
			eventName:  eventName,
			expected:   []string{"dc1.automation-testPlainEventGlobal", "dc2.automation-testPlainEventGlobal"},
		},
	}
	// Running all positive tests twice for regression
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualList, err := GetAllDCTopicName(tt.serviceUrl, tt.appName, tt.eventName)
				assert.Equal(t, tt.expected, actualList, "Unexpected topic name detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
}

func TestGetConsumerGroupName(t *testing.T) {
	positiveTests := []struct {
		desc      string
		appName   string
		eventName entry.EventEntry
		expected  string
	}{
		{
			desc:    "Testing the consumer group name concatenation",
			appName: consumerAppName,
			eventName: entry.EventEntry{
				AppName:   appName,
				EventName: eventName,
			},
			expected: "automationNew-automation-testPlainEventGlobal-consumer-group",
		},
	}
	// Running all positive tests twice for regression
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualList, err := GetConsumerGroupName(tt.appName, &tt.eventName)
				assert.Equal(t, tt.expected, actualList, "Unexpected topic name detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
}

func TestGetTopicDetailsCache(t *testing.T) {
	positiveTests := []struct {
		desc       string
		serviceUrl string
		topicName  string
		expected   entry.TopicEntry
	}{
		{
			desc:       "Testing the behavior with service url of DC2",
			serviceUrl: serviceUrlDC2,
			topicName:  appName + "-" + eventName,
			expected: entry.TopicEntry{
				EventName:      "testPlainEventGlobal",
				AppName:        "automation",
				SchemaId:       -1,
				Status:         "SUBSCRIBE",
				SchemaType:     "PLAIN",
				Profile:        "MEDIUM",
				Replication:    entry.Global,
				IsBCPCompliant: true,
				Id:             330,
			},
		},
	}
	// Running all positive tests twice for regression
	for i := 0; i < 4; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualList, err := GetTopicDetailsCache(tt.serviceUrl, tt.topicName)
				assert.Equal(t, tt.expected, *actualList, "Unexpected topic name detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
}

func TestGetConsumerOffsetConfig(t *testing.T) {
	positiveTests := []struct {
		desc              string
		serviceUrl        string
		consumerGroupName string
		dcprefix          string
	}{
		{
			desc:              "Testing the offset entry fetch with service url of DC2",
			serviceUrl:        serviceUrlDC2,
			consumerGroupName: "EORSC-EORSP-pavan-consumer-group",
			dcprefix:          "dc1",
		},
	}
	// Running all positive tests twice for regression
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualList, err := GetConsumerOffsetConfig(tt.serviceUrl, tt.consumerGroupName)
				assert.Equal(t, tt.consumerGroupName, actualList[0].ConsumerGroupName, "Unexpected consumer group name found.")
				assert.Equal(t, tt.consumerGroupName, actualList[0].Id, "Unexpected consumer group name found.")
				assert.Equal(t, tt.dcprefix, actualList[0].DCPrefix, "Wrong dc prefix found")
				assert.True(t, actualList[0].Offset > 0, "Offset should be a positive value")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
	negativeTests := []struct {
		desc              string
		serviceUrl        string
		consumerGroupName string
		expectedError     string
	}{
		{
			desc:              "Testing with random consumer group name for error",
			serviceUrl:        serviceUrlDC2,
			consumerGroupName: "randomname-starts-here",
			expectedError:     constants.ErrorOffsetConfigNotFound,
		},
	}
	// Running all negative tests twice for regression
	for i := 2; i < 4; i++ {
		for _, tt := range negativeTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualList, err := GetConsumerOffsetConfig(tt.serviceUrl, tt.consumerGroupName)
				assert.NotNil(t, err, "Error should not be nil")
				assert.Nil(t, actualList, "entry should not exist for random name")
				assert.Equal(t, tt.expectedError, err.Error())
			})
		}
	}
}

func TestGetConcurrencyLimit(t *testing.T) {
	positiveTests := []struct {
		desc          string
		serviceUrl    string
		expectedLimit int
	}{
		{
			desc:          "Testing the behavior for concurrency limit config",
			serviceUrl:    serviceUrlDC1,
			expectedLimit: 9,
		},
	}
	// Running all positive tests twice
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualPrefix, err := GetConcurrencyLimit(tt.serviceUrl)
				assert.Equal(t, tt.expectedLimit, actualPrefix, "Unexpected JSON detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
}
