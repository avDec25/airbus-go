package consumer

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	serviceUrlDC1   = `http://fcp-airbusreplicator2:7071`
	globalAppName   = `kohli`
	globalEventName = `sachin`
	locaAppName     = `kohli`
	localEventName  = `dhoni`
	oldAppName      = `kohli`
	oldEventName    = `dravid`
	metaAppName     = `airbus`
	metaEventName   = `registeration`
)

func TestGetTopics(t *testing.T) {
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
			appName:        globalAppName,
			eventName:      globalEventName,
			expectedPrefix: []string{"dc1.kohli-sachin", "dc2.kohli-sachin"},
		},
		{
			desc:           "Testing the behavior for local consumer",
			serviceUrl:     serviceUrlDC1,
			appName:        locaAppName,
			eventName:      localEventName,
			expectedPrefix: []string{"dc1.kohli-dhoni"},
		},
		{
			desc:           "Testing the behavior for old consumer",
			serviceUrl:     serviceUrlDC1,
			appName:        oldAppName,
			eventName:      oldEventName,
			expectedPrefix: []string{"kohli-dravid"},
		},
		{
			desc:           "Testing the behavior for meta consumer",
			serviceUrl:     serviceUrlDC1,
			appName:        metaAppName,
			eventName:      metaEventName,
			expectedPrefix: []string{"dc1.airbus-registeration", "dc2.airbus-registeration", "airbus-registeration"},
		},
	}
	// Running all positive tests twice
	for i := 0; i < 2; i++ {
		for _, tt := range positiveTests {
			t.Run(tt.desc+" Run : "+strconv.Itoa(i), func(t *testing.T) {
				actualPrefix, err := getTopics(tt.serviceUrl, tt.appName, tt.appName, tt.eventName)
				assert.Equal(t, tt.expectedPrefix, actualPrefix, "Unexpected JSON detected.")
				assert.Nil(t, err, "Error should be nil")
			})
		}
	}
}
