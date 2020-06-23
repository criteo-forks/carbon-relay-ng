package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateParentDirectory(t *testing.T) {
	parents := NewMetricDirectory("a.b.c").generateParentDirectories()
	assert.Equal(t, parents[0].name, "a")
	assert.Equal(t, parents[1].name, "a.b")
	assert.Equal(t, parents[2].name, "a.b.c")

}

func TestUpdateDirectories(t *testing.T) {
	conn := &BgMetadataTestingStorageConnector{make([]string, 0, 3), make([]string, 0, 3), make([]string, 0, 3), sync.Mutex{}}
	dir := NewMetricDirectory("a.b.c")
	dir.UpdateDirectories(conn)

	assert.Contains(t, conn.UpdatedDirectories, "a")
	assert.Contains(t, conn.UpdatedDirectories, "a.b")
	assert.Contains(t, conn.UpdatedDirectories, "a.b.c")

	assert.Contains(t, conn.SelectedDirectories, "a")
	assert.Contains(t, conn.SelectedDirectories, "a.b")
	assert.Contains(t, conn.SelectedDirectories, "a.b.c")

	assert.Equal(t, 3, len(conn.UpdatedDirectories))
	assert.Equal(t, 3, len(conn.SelectedDirectories))
}
