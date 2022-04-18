package producer

import (
	"github.com/avDec25/airbus-go/entry"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	appName       = `kohli`
	bcpEventName  = `sachin`
	oldEventName  = `dravid`
	serviceUrlDC1 = `http://fcp-airbusreplicator2:7071`
)

func TestGetTopic(t *testing.T) {
	positiveTests := []struct {
		desc           string
		serviceUrl     string
		topicEntry     *entry.TopicEntry
		expectedPrefix string
	}{
		{
			desc:       "Testing the behavior for old producer",
			serviceUrl: serviceUrlDC1,
			topicEntry: &entry.TopicEntry{
				IsBCPCompliant: false,
				AppName:        appName,
				EventName:      oldEventName,
			},
			expectedPrefix: "kohli-dravid",
		},
		{
			desc:       "Testing the behavior for bcp producer",
			serviceUrl: serviceUrlDC1,
			topicEntry: &entry.TopicEntry{
				IsBCPCompliant: true,
				AppName:        appName,
				EventName:      bcpEventName,
			},
			expectedPrefix: "dc1.kohli-sachin",
		},
	}
	// Running all positive tests twice
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualPrefix, err := getTopicName(tt.serviceUrl, tt.topicEntry)
				assert.Equal(t, tt.expectedPrefix, actualPrefix, "Unexpected JSON detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
}
