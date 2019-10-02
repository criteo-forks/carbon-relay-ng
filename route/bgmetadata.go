package route

import (
	"context"
	"hash/crc32"
	"sync"
	"time"

	"github.com/graphite-ng/carbon-relay-ng/encoding"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/willf/bloom"
	"go.uber.org/zap"
)

type shard struct {
	filter  bloom.BloomFilter
	channel chan string
	lock    sync.RWMutex
}

// BloomFilterConfig contains filter size and false positive chance for all bloom filters
type BloomFilterConfig struct {
	n              uint
	p              float64
	shardingFactor int
	bufferSize     int
	clearInterval  time.Duration
	clearWait      time.Duration
	logger         *zap.Logger
}

// BgMetadata contains data required to start, stop and reset a metric metadata producer.
type BgMetadata struct {
	baseRoute
	shards []shard
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	bfCfg  BloomFilterConfig
}

// NewBloomFilterConfig creates a new BloomFilterConfig
func NewBloomFilterConfig(n uint, p float64, shardingFactor int, clearInterval, clearWait time.Duration) (*BloomFilterConfig, error) {
	bfc := BloomFilterConfig{
		n:              n,
		p:              p,
		shardingFactor: shardingFactor,
		bufferSize:     500,
		clearInterval:  clearInterval,
		clearWait:      clearWait,
		logger:         zap.L(),
	}
	if clearWait != 0 {
		bfc.clearWait = clearWait
	} else {
		bfc.clearWait = clearInterval / time.Duration(shardingFactor)
		bfc.logger.Debug("overiding clear_wait value", zap.Duration("clear_wait", bfc.clearWait))
	}
	return &bfc, nil
}

// NewBgMetadataRoute creates BgMetadata, starts sharding and filtering incoming metrics.
// Runs a goroutines for each shard to handle incoming metrics and periodic cleanup of bloom filters
func NewBgMetadataRoute(key, prefix, sub, regex string, bfCfg *BloomFilterConfig) (*BgMetadata, error) {
	m := BgMetadata{
		baseRoute: *newBaseRoute(key, "bg_metadata"),
		shards:    make([]shard, bfCfg.shardingFactor),
		bfCfg:     *bfCfg,
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	// init every shard with filter and channel
	for shardNum := 0; shardNum < bfCfg.shardingFactor; shardNum++ {
		m.shards[shardNum] = shard{
			filter:  *bloom.NewWithEstimates(bfCfg.n, bfCfg.p),
			channel: make(chan string, bfCfg.bufferSize),
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
	m.logger.Debug("starting goroutine for handling received metrics", zap.Int("shard_number", shardNum))
	shard := &m.shards[shardNum]
	for {
		select {
		case <-m.ctx.Done():
			close(shard.channel)
			return
		case metric := <-shard.channel:
			if !shard.filter.TestString(metric) {
				shard.filter.AddString(metric)
				// increase outgoing metric prometheus counter
				m.rm.OutMetrics.Inc()
				// do nothing for now
				m.logger.Debug("adding new metric to bloom filter", zap.String("name", metric))
			} else {
				// don't output metrics already in the filter
				m.logger.Debug("skipping metric already present in bloom filter", zap.String("name", metric))
			}
		}
	}
}

func (m *BgMetadata) clearBloomFilter() {
	m.logger.Info("starting goroutine for bloom filter cleanup")
	m.wg.Add(1)
	defer m.wg.Done()
	t := time.NewTicker(m.bfCfg.clearInterval)
	defer t.Stop()

	for {
		select {
		case <-m.ctx.Done():
			t.Stop()
			return
		case <-t.C:
			for i := range m.shards {
				select {
				case <-m.ctx.Done():
					t.Stop()
					return
				default:
					m.shards[i].lock.Lock()
					m.shards[i].filter.ClearAll()
					m.logger.Info("clearing filter for shard", zap.Int("shard_number", i+1))
					m.shards[i].lock.Unlock()
					time.Sleep(m.bfCfg.clearWait)
				}
			}
		}
	}
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
	shardNum := crc32.ChecksumIEEE([]byte(dp.Name)) % uint32(m.bfCfg.shardingFactor)
	m.shards[shardNum].channel <- dp.Name
	return
}

func (m *BgMetadata) Snapshot() Snapshot {
	return makeSnapshot(&m.baseRoute)
}
