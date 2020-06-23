package storage

import (
	"fmt"
	"sync"
)

type BgMetadataStorageConnector interface {
	UpdateMetricMetadata(metric *Metric) error
	InsertDirectory(dir *MetricDirectory) error
	SelectDirectory(dir string) (string, error) // SelectDirectory returns the parent directory or an error if it is not created
	// empty string means root directory
}

// default connector, does nothing used for testing
type BgMetadataTestingStorageConnector struct {
	UpdatedDirectories  []string
	SelectedDirectories []string
	UpdatedMetrics      []string
	mu                  sync.Mutex
}

func (cc *BgMetadataTestingStorageConnector) UpdateMetricMetadata(metric *Metric) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.UpdatedMetrics = append(cc.UpdatedMetrics, metric.name)
	return nil
}

func (cc *BgMetadataTestingStorageConnector) InsertDirectory(dir *MetricDirectory) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if dir.name != "" {
		cc.UpdatedDirectories = append(cc.UpdatedDirectories, dir.name)
	}
	return nil
}

func (cc *BgMetadataTestingStorageConnector) SelectDirectory(dir string) (string, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.SelectedDirectories = append(cc.SelectedDirectories, dir)
	return dir, fmt.Errorf("Directory not found")
}

// default connector, really does nothing used for testing
type BgMetadataNoOpStorageConnector struct {
}

func (cc *BgMetadataNoOpStorageConnector) UpdateMetricMetadata(metric *Metric) error {
	return nil
}

func (cc *BgMetadataNoOpStorageConnector) InsertDirectory(dir *MetricDirectory) error {
	return nil
}

func (cc *BgMetadataNoOpStorageConnector) SelectDirectory(dir string) (string, error) {
	return dir, fmt.Errorf("Directory not found")
}
