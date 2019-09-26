package route

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/graphite-ng/carbon-relay-ng/encoding"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/willf/bloom"
)

type shard struct {
	filter  bloom.BloomFilter
	channel chan []byte
	lock    sync.RWMutex
}

// BloomFilterConfig contains filter size and false positive chance for all bloom filters
type BloomFilterConfig struct {
	N uint
	P float64
}

// BgMetadata contains data required to start, stop and reset a metric metadata producer.
type BgMetadata struct {
	baseRoute
	shards        []shard
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	clearInterval time.Duration
	clearWait     time.Duration
}

// NewBgMetadataRoute creates BgMetadata, starts sharding and filtering incoming metrics.
// Runs a goroutines for each shard to handle incoming metrics and periodic cleanup of bloom filters
func NewBgMetadataRoute(key, prefix, sub, regex string, shardingFactor int, clearInterval, clearWait time.Duration, bloomFilterCfg *BloomFilterConfig) (*BgMetadata, error) {
	m := BgMetadata{
		baseRoute:     *newBaseRoute(key, "bg_metadata"),
		shards:        make([]shard, shardingFactor),
		clearInterval: clearInterval,
	}

	if clearWait != 0 {
		m.clearWait = clearWait
	} else {
		m.clearWait = clearInterval / time.Duration(shardingFactor)
		m.logger.Info(fmt.Sprintf("setting clear_wait value to %s", m.clearWait.String()))
	}

	m.ctx, m.cancel = context.WithCancel(context.Background())

	// init every shard with filter and channel
	for shardNum := 0; shardNum < shardingFactor; shardNum++ {
		m.shards[shardNum] = shard{
			filter:  *bloom.NewWithEstimates(bloomFilterCfg.N, bloomFilterCfg.P),
			channel: make(chan []byte, 500),
		}
		go m.handleMetric(shardNum)
	}
	m.logger.Info("all metric handlers started")

	go m.clearBloomFilter()

	// matcher required to initialise route.Config for routing table, othewise it will panic
	mt, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	m.config.Store(baseConfig{*mt, nil})

	return &m, nil
}

func (m *BgMetadata) handleMetric(shardNum int) {
	m.wg.Add(1)
	defer m.wg.Done()
	m.logger.Debug(fmt.Sprintf("starting metric handler for shard %d", shardNum+1))
	for {
		select {
		case <-m.ctx.Done():
			close(m.shards[shardNum].channel)
			return
		case metric := <-m.shards[shardNum].channel:
			if !m.shards[shardNum].filter.Test(metric) {
				m.shards[shardNum].filter.Add(metric)
				// increase outgoing metric prometheus counter
				m.rm.OutMetrics.Inc()
				// do nothing for now
				m.logger.Debug(fmt.Sprintf("adding metric to metadata: %s", string(metric)))
			} else {
				// don't output metrics already in the filter
				m.logger.Debug(fmt.Sprintf("skipping metric: %s", string(metric)))
			}
		}
	}
}

func (m *BgMetadata) clearBloomFilter() {
	m.logger.Info("starting bloom filter clear goroutine")
	m.wg.Add(1)
	defer m.wg.Done()
	t := time.NewTicker(m.clearInterval)
	defer t.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
			for i := range m.shards {
				m.shards[i].lock.Lock()
				m.shards[i].filter.ClearAll()
				m.logger.Debug(fmt.Sprintf("clearing all filters for shard %d", i+1))
				m.shards[i].lock.Unlock()
				time.Sleep(m.clearWait)
			}
		}
	}
}

func getShardNum(metricName string, shardingFactor int) uint32 {
	return crc32.Checksum([]byte(metricName), crc32.MakeTable(crc32.IEEE)) % uint32(shardingFactor)
}

// Shutdown cancels the context used in BgMetadata and goroutines
// It waits for goroutines to close channels and finish before exiting
func (m *BgMetadata) Shutdown() error {
	m.logger.Info("shutting down bg_metadata")
	m.cancel()
	m.logger.Debug("waiting for goroutines")
	m.wg.Wait()
	return nil
}

// Dispatch puts each datapoint metric name in a shard channel accordingly
// The channel is determined based on the name crc32 hash and sharding factor
func (m *BgMetadata) Dispatch(dp encoding.Datapoint) {
	// increase incoming metric prometheus counter
	m.rm.InMetrics.Inc()
	shardNum := getShardNum(dp.Name, len(m.shards))
	m.shards[shardNum].channel <- []byte(dp.Name)
	return
}

func (m *BgMetadata) Snapshot() Snapshot {
	return makeSnapshot(&m.baseRoute)
}
