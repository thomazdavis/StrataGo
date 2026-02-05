package sstable

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thomazdavis/flashdb/memtable"
)

func TestBuilder_Flush(t *testing.T) {
	filename := "test_level0.sst"
	defer os.Remove(filename)

	list := memtable.NewSkipList()
	list.Put([]byte("Barca:1"), []byte("Spain"))
	list.Put([]byte("Bayern:2"), []byte("Deutschland"))

	builder, err := NewBuilder(filename)
	assert.NoError(t, err)

	err = builder.Flush(list)
	assert.NoError(t, err)

	info, err := os.Stat(filename)
	assert.NoError(t, err)
	assert.True(t, info.Size() > 0, "SSTable file should not be empty")
}

func TestEngine_RotationSimulation(t *testing.T) {
	sstFile := "test_rotation.sst"
	walFile := "test_rotation.log"
	defer os.Remove(sstFile)
	defer os.Remove(walFile)

	list := memtable.NewSkipList()
	list.Put([]byte("key"), []byte("val"))

	builder, err := NewBuilder(sstFile)
	assert.NoError(t, err)
	err = builder.Flush(list)
	assert.NoError(t, err)

	f, _ := os.Create(walFile)
	f.Close()

	err = os.Remove(walFile)
	assert.NoError(t, err, "Should be able to delete old WAL")
}
