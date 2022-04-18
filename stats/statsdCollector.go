package stats

import (
	"github.com/cactus/go-statsd-client/statsd"
	"time"
)

type StatsdCollector interface {
	IncrementCounter(prefix string)
	Gauge(prefix string, value int64)
	Close()
}

type statsdCollectorClient struct {
	client     statsd.Statter
	sampleRate float32
}

// https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets
const (
	WANStatsdFlushBytes     = 512
	LANStatsdFlushBytes     = 1432
	GigabitStatsdFlushBytes = 8932
)

// StatsdCollectorConfig provides configuration that the Statsd client will need.
type StatsdCollectorConfig struct {
	// StatsdAddr is the tcp address of the Statsd server
	StatsdAddr string
	// Prefix is the prefix that will be prepended to all metrics sent from this collector.
	Prefix string
	// StatsdSampleRate sets statsd sampling. If 0, defaults to 1.0. (no sampling)
	SampleRate float32
	// FlushBytes sets message size for statsd packets. If 0, defaults to LANFlushSize.
	FlushBytes int
}

// InitializeStatsdCollector creates the connection to the Statsd server
// and should be called before any metrics are recorded.
//
// Users should ensure to call Close() on the client.
func InitializeStatsdCollector(config *StatsdCollectorConfig) (StatsdCollector, error) {
	flushBytes := config.FlushBytes
	if flushBytes == 0 {
		flushBytes = LANStatsdFlushBytes
	}

	sampleRate := config.SampleRate
	if sampleRate == 0 {
		sampleRate = 1
	}

	c, err := statsd.NewBufferedClient(config.StatsdAddr, config.Prefix, 1*time.Second, flushBytes)
	if err != nil {
		log.Errorf("Could not initiate buffered client: %s. Falling back to a Noop Statsd client", err)
		c, _ = statsd.NewNoopClient()
	}
	return &statsdCollectorClient{
		client:     c,
		sampleRate: sampleRate,
	}, err
}

func (this *statsdCollectorClient) IncrementCounter(prefix string) {
	err := this.client.Inc(prefix, 1, this.sampleRate)
	if err != nil {
		log.Errorf("Error sending statsd metrics %s\n", prefix)
	}
}

func (this *statsdCollectorClient) Gauge(prefix string, value int64) {
	err := this.client.Gauge(prefix, value, this.sampleRate)
	if err != nil {
		log.Errorf("Error sending statsd metrics %s\n", prefix)
	}
}

func (this *statsdCollectorClient) Close() {
	err := this.client.Close()
	if err != nil {
		log.Errorf("Error closing statsd client %s\n", err.Error())
	}
}
