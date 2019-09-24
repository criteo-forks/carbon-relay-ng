package route

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/graphite-ng/carbon-relay-ng/encoding"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/willf/bloom"
)

type BloomFilterConfig struct {
	N uint
	P float64
}

type BgMetadata struct {
	baseRoute
	metricChannels []chan []byte
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

func NewBgMetadataRoute(key, prefix, sub, regex string, shards int, bloomFilterCfg *BloomFilterConfig) (*BgMetadata, error) {
	m := BgMetadata{
		baseRoute: *newBaseRoute(key, "bg_metadata"),
	}
	// create a channel for every shard
	m.metricChannels = make([]chan []byte, shards)
	for shard := range m.metricChannels {
		m.metricChannels[shard] = make(chan []byte, 500)
	}
	// create context
	m.ctx, m.cancel = context.WithCancel(context.Background())
	// start goroutines to handle metric filtering
	for shard, ch := range m.metricChannels {
		go m.handleMetric(m.ctx, ch, shard, bloomFilterCfg)
	}

	// matcher required to initialise route.Config for routing table
	// othewise it will panic
	mt, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	m.config.Store(baseConfig{*mt, nil})

	return &m, nil
}

func (m *BgMetadata) handleMetric(ctx context.Context, ch chan []byte, shard int, bloomFilterCfg *BloomFilterConfig) {
	m.wg.Add(1)
	defer m.wg.Done()
	m.logger.Debug(fmt.Sprintf("starting metric handler for shard %d", shard+1))
	// init bloom filter
	bloomFilter := bloom.NewWithEstimates(bloomFilterCfg.N, bloomFilterCfg.P)
	// read from channel and fill filter
	for {
		select {
		case <-ctx.Done():
			// close channel when done
			close(ch)
			return
		case metric := <-ch:
			// add to bloom filter if not there already
			if !bloomFilter.Test(metric) {
				bloomFilter.Add(metric)
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

func getChannelNum(metricName string, chLength int) uint32 {
	return crc32.Checksum([]byte(metricName), crc32.MakeTable(crc32.IEEE)) % uint32(chLength)
}

func (m *BgMetadata) Shutdown() error {
	// start close and cleanup of everything here; and return err
	m.logger.Info("shutting down bg_metadata")
	m.cancel()
	m.logger.Debug("waiting for goroutines")
	m.wg.Wait()
	return nil
}

func (m *BgMetadata) Dispatch(dp encoding.Datapoint) {
	// increase incoming metric prometheus counter
	m.rm.InMetrics.Inc()
	// crc32 mod shard number
	chNum := getChannelNum(dp.Name, len(m.metricChannels))
	// put metric in channels handled in goroutines
	m.metricChannels[chNum] <- []byte(dp.Name)
	return
}

func (m *BgMetadata) Snapshot() Snapshot {
	return makeSnapshot(&m.baseRoute)
}
