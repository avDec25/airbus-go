package metadata

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	appName       = `test`
	eventName     = `testevent`
	serviceUrlDC1 = `http://platformairbus.stage.myntra.com`
)

func TestGetAllTopics(t *testing.T) {
	positiveTests := []struct {
		desc           string
		serviceUrl     string
		appName        string
		eventName      string
		expectedPrefix []string
	}{
		{
			desc:           "Testing the behavior for global consumer",
			serviceUrl:     serviceUrlDC1,
			appName:        appName,
			eventName:      eventName,
			expectedPrefix: []string{"dc1.test-testevent", "dc2.test-testevent", "test-testevent"},
		},
	}
	// Running all positive tests twice
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualPrefix, err := getAllTopics(tt.serviceUrl, tt.appName, tt.eventName)
				assert.Equal(t, tt.expectedPrefix, actualPrefix, "Unexpected JSON detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
}
