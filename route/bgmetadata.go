package route

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/graphite-ng/carbon-relay-ng/cfg"
	"github.com/graphite-ng/carbon-relay-ng/encoding"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	"github.com/graphite-ng/carbon-relay-ng/metrics"
	"github.com/graphite-ng/carbon-relay-ng/storage"
	"github.com/willf/bloom"
	"go.uber.org/zap"
)

type shard struct {
	num    int
	filter bloom.BloomFilter
	lock   sync.RWMutex
}

// BloomFilterConfig contains filter size and false positive chance for all bloom filters
type BloomFilterConfig struct {
	N              uint
	P              float64
	ShardingFactor int
	Cache          string
	ClearInterval  time.Duration
	SaveInterval   time.Duration
	ClearWait      time.Duration
	logger         *zap.Logger
}

// BgMetadata contains data required to start, stop and reset a metric metadata producer.
type BgMetadata struct {
	baseRoute
	shards              []shard
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	bfCfg               BloomFilterConfig
	mm                  metrics.BgMetadataMetrics
	metricDirectories   chan string
	storageSchemas      []storage.StorageSchema
	storageAggregations []storage.StorageAggregation
	storage             storage.BgMetadataStorageConnector
}

// NewBloomFilterConfig creates a new BloomFilterConfig
func NewBloomFilterConfig(n uint, p float64, shardingFactor int, cache string, clearInterval, clearWait, saveInterval time.Duration) (BloomFilterConfig, error) {
	bfc := BloomFilterConfig{
		N:              n,
		P:              p,
		ShardingFactor: shardingFactor,
		Cache:          cache,
		ClearInterval:  clearInterval,
		ClearWait:      clearWait,
		SaveInterval:   saveInterval,
		logger:         zap.L(),
	}
	if clearWait != 0 {
		bfc.ClearWait = clearWait
	} else {
		bfc.ClearWait = clearInterval / time.Duration(shardingFactor)
		bfc.logger.Warn("overiding clear_wait value", zap.Duration("clear_wait", bfc.ClearWait))
	}
	return bfc, nil
}

// NewBgMetadataRoute creates BgMetadata, starts sharding and filtering incoming metrics.
// additionnalCfg should be nil or *cfg.BgMetadataESConfig if elasticsearch
func NewBgMetadataRoute(key, prefix, sub, regex, aggregationCfg, schemasCfg string, bfCfg BloomFilterConfig, storageName string, additionnalCfg interface{}) (*BgMetadata, error) {
	// to make value assignments easier
	var err error

	m := BgMetadata{
		baseRoute:         *newBaseRoute(key, "bg_metadata"),
		shards:            make([]shard, bfCfg.ShardingFactor),
		bfCfg:             bfCfg,
		metricDirectories: make(chan string),
	}

	// load schema and aggregation configuration files
	m.storageAggregations, err = storage.NewStorageAggregations(aggregationCfg)
	if err != nil {
		return &m, err
	}
	m.storageSchemas, err = storage.NewStorageSchemas(schemasCfg)
	if err != nil {
		return &m, err
	}

	// init every shard with filter
	for shardNum := 0; shardNum < bfCfg.ShardingFactor; shardNum++ {
		m.shards[shardNum] = shard{
			num:    shardNum,
			filter: *bloom.NewWithEstimates(bfCfg.N, bfCfg.P),
		}
	}

	if m.bfCfg.Cache != "" {
		err := m.createCacheDirectory()
		if err != nil {
			return &m, err
		}
		err = m.validateCachedFilterConfig()
		if err != nil {
			m.logger.Warn("cache validation failed", zap.Error(err))
			err = m.deleteCache()
			if err != nil {
				return &m, err
			}
			err = m.saveFilterConfig()
			if err != nil {
				return &m, err
			}
		} else {
			m.logger.Info("loading cached filter states")
			for shardNum := 0; shardNum < bfCfg.ShardingFactor; shardNum++ {
				err = m.shards[shardNum].loadShardState(*m.logger, bfCfg.Cache)
				if err != nil {
					// skip state loading if file doesn't exist or cannot be loaded
					m.logger.Warn("cannot load state file", zap.Int("shard", shardNum), zap.Error(err))
				}
			}
		}

		go m.saveBloomFilter()
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())

	m.mm = metrics.NewBgMetadataMetrics(key)
	m.mm.BloomFilterMaxEntries.Set(float64(bfCfg.N))
	m.rm = metrics.NewRouteMetrics(key, "bg_metadata", nil)

	go m.clearBloomFilter()
	switch storageName {
	case "cassandra":
		// WIP
		m.storage = storage.NewCassandraMetadata()
	case "elasticsearch":
		if v, ok := additionnalCfg.(*cfg.BgMetadataESConfig); ok == true {
			m.storage = storage.NewBgMetadataElasticSearchConnectorWithDefaults(v)
		} else {
			return &m, fmt.Errorf("missing elasticsearch configuration")
		}
	case "noop":
		m.storage = &storage.BgMetadataNoOpStorageConnector{}
	case "testing":
		m.storage = &storage.BgMetadataTestingStorageConnector{}
	default:
		log.Fatalf("unknown metadata backend")
	}

	// matcher required to initialise route.Config for routing table, othewise it will panic
	mt, err := matcher.New(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	m.config.Store(baseConfig{*mt, nil})

	return &m, nil
}

func (s *shard) loadShardState(logger zap.Logger, cachePath string) error {
	file, err := os.Open(filepath.Join(cachePath, fmt.Sprintf("shard%d.state", s.num)))
	if err != nil {
		return err
	}
	defer file.Close()
	logger.Debug("decoding shard state file", zap.String("filename", file.Name()))
	err = gob.NewDecoder(file).Decode(&s.filter)
	if err != nil {
		return err
	}
	return nil
}

func (s *shard) saveShardState(cachePath string) error {
	stateFile, err := os.Create(filepath.Join(cachePath, fmt.Sprintf("shard%d.state", s.num)))
	if err != nil {
		return err
	}
	defer stateFile.Close()
	err = gob.NewEncoder(stateFile).Encode(&s.filter)
	if err != nil {
		return err
	}
	return nil
}

// Remove Shard State file from cache and return an error if file exists and but fails to be removed.
func (s *shard) removeShardState(cachePath string) error {
	fp := filepath.Join(cachePath, fmt.Sprintf("shard%d.state", s.num))

	if _, err := os.Stat(fp); !os.IsNotExist(err) {
		return os.Remove(fp)
	}

	return nil
}

// Continuously save bloom filters
func (m *BgMetadata) saveBloomFilter() {
	var err error

	m.logger.Debug("Starting goroutine for bloom filter saving")
	m.wg.Add(1)
	defer m.wg.Done()

	t := time.NewTicker(m.bfCfg.SaveInterval)
	for {
		select {
		case <-m.ctx.Done():
			t.Stop()
			return
		case <-t.C:
			m.logger.Debug("saveBloomFilter: Saving shards.")
			for i := range m.shards {
				shard := &m.shards[i]
				shard.lock.Lock()

				err = shard.saveShardState(m.bfCfg.Cache)
				if err != nil {
					m.logger.Error("cannot save shard state to filesystem", zap.Error(err))
				}

				shard.lock.Unlock()
			}
			m.logger.Debug("saveBloomFilter: Done saving all shards.")
		}
	}
}

func (m *BgMetadata) clearBloomFilter() {
	m.logger.Debug("starting goroutine for bloom filter cleanup")
	m.wg.Add(1)
	defer m.wg.Done()
	t := time.NewTicker(m.bfCfg.ClearWait)
	for {
		for i := range m.shards {
			select {
			case <-m.ctx.Done():
				t.Stop()
				return
			case <-t.C:
				m.clearBloomFilterShard(i)
			}
		}
	}
}

func (m *BgMetadata) clearBloomFilterShard(shardNum int) {
	m.logger.Debug("Cleaning Bloom Filter Shard", zap.Int("shard_number", shardNum+1))
	sh := &m.shards[shardNum]
	sh.lock.Lock()
	m.logger.Info("clearing filter for shard", zap.Int("shard_number", shardNum+1))
	sh.filter.ClearAll()
	m.mm.BloomFilterEntries.DeleteLabelValues(strconv.Itoa(sh.num))
	sh.lock.Unlock()
	time.Sleep(m.bfCfg.ClearWait)
}

func (m *BgMetadata) saveFilterConfig() error {
	configPath := filepath.Join(m.bfCfg.Cache, "bloom_filter_config")
	file, err := os.Create(configPath)
	if err != nil {
		m.logger.Warn("cannot save bloom filter config to filesystem", zap.Error(err))
		return err
	}
	defer file.Close()
	m.logger.Info("saving current filter config", zap.String("file", configPath))
	return gob.NewEncoder(file).Encode(&m.bfCfg)
}

func (m *BgMetadata) validateCachedFilterConfig() error {
	configPath := filepath.Join(m.bfCfg.Cache, "bloom_filter_config")
	file, err := os.Open(configPath)
	if err != nil {
		m.logger.Warn("cannot read bloom filter config", zap.Error(err))
		return err
	}
	defer file.Close()
	loadedConfig := BloomFilterConfig{}
	err = gob.NewDecoder(file).Decode(&loadedConfig)
	if err != nil {
		m.logger.Warn("cannot decode bloom filter config", zap.Error(err))
		return err
	}
	if cmp.Equal(loadedConfig, m.bfCfg, cmpopts.IgnoreUnexported(BloomFilterConfig{})) {
		m.logger.Info("cached bloom filter config matches current")
		m.bfCfg = loadedConfig
		return nil
	}
	return errors.New("cached config does not match current config")
}

func (m *BgMetadata) createCacheDirectory() error {
	f, err := os.Stat(m.bfCfg.Cache)
	if err != nil {
		if os.IsNotExist(err) {
			m.logger.Info("creating directory for cache", zap.String("path", m.bfCfg.Cache))
			mkdirErr := os.MkdirAll(m.bfCfg.Cache, os.ModePerm)
			if mkdirErr != nil {
				m.logger.Error("cannot create cache directory", zap.String("path", m.bfCfg.Cache))
				return mkdirErr
			}
			return nil
		}
		m.logger.Error("cannot not check cache directory", zap.String("path", m.bfCfg.Cache))
		return err
	}
	if !f.IsDir() {
		m.logger.Error("cache path already exists but is not a directory", zap.String("path", m.bfCfg.Cache))
		return err
	}
	m.logger.Info("cache path already exists", zap.String("path", m.bfCfg.Cache))
	return nil
}

func (m *BgMetadata) deleteCache() error {
	m.logger.Warn("deleting cached state and configuration files", zap.String("path", m.bfCfg.Cache))
	names, err := ioutil.ReadDir(m.bfCfg.Cache)
	if err != nil {
		return err
	}
	for _, entry := range names {
		os.RemoveAll(path.Join([]string{m.bfCfg.Cache, entry.Name()}...))
	}
	return nil
}

// testStringAndAdd will check if string present in bloom filters and add it if it's not
// returns whether the string was in the BF or not
// The shard is determined based on the name crc32 hash and sharding factor
func (m *BgMetadata) testStringAndAdd(name string) bool {
	shardNum := crc32.ChecksumIEEE([]byte(name)) % uint32(m.bfCfg.ShardingFactor)
	shard := &m.shards[shardNum]
	shard.lock.Lock()
	defer shard.lock.Unlock()
	if !shard.filter.TestString(name) {
		m.mm.BloomFilterEntries.WithLabelValues(strconv.Itoa(shard.num)).Inc()
		shard.filter.AddString(name)
		return false
	}
	return true

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

// Dispatch puts each datapoint metric name in a bloom filter
func (m *BgMetadata) Dispatch(dp encoding.Datapoint) {
	// increase incoming metric prometheus counter
	m.rm.InMetrics.Inc()
	if !m.testStringAndAdd(dp.Name) {
		m.mm.AddedMetrics.Inc()
		m.DispatchDirectory(dp)
		metricMetadata := storage.NewMetricMetadata(dp.Name, m.storageSchemas, m.storageAggregations)
		metric := storage.NewMetric(dp.Name, metricMetadata, dp.Tags)
		m.storage.UpdateMetricMetadata(metric)

	} else {
		// don't output metrics already in the filter
		m.mm.FilteredMetrics.Inc()
	}
	return
}

// DispatchDirectory puts each datapoint directory name in a bloom filter
func (m *BgMetadata) DispatchDirectory(dp encoding.Datapoint) {
	dirname, err := dp.Directory()
	if err != nil {
		m.logger.Warn("No directory", zap.Error(err))
	}

	if !m.testStringAndAdd(dirname) {
		directory := storage.NewMetricDirectory(dirname)
		directory.UpdateDirectories(m.storage)
	}
}

func (m *BgMetadata) Snapshot() Snapshot {
	return makeSnapshot(&m.baseRoute)
}
