package flashdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlashDB_Integration(t *testing.T) {
	dataDir := "test_data"
	defer os.RemoveAll(dataDir)

	// Test Open and Put
	db, err := Open(dataDir)
	assert.NoError(t, err)

	err = db.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	// Test Get from Memtable
	val, found := db.Get([]byte("key1"))
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), val)

	// Test Flush
	err = db.Flush()
	assert.NoError(t, err)

	// Test Get from SSTable (Memtable is now empty)
	val, found = db.Get([]byte("key1"))
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), val)

	// Test Recovery
	db.Close()
	db2, err := Open(dataDir)
	assert.NoError(t, err)
	defer db2.Close()

	val, found = db2.Get([]byte("key1"))
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), val)
}

func TestFlashDB_ConcurrentAccess(t *testing.T) {
	dataDir := "concurrent_test"
	defer os.RemoveAll(dataDir)

	db, _ := Open(dataDir)
	defer db.Close()

	done := make(chan bool)

	// Background writer
	go func() {
		for range 100 {
			db.Put([]byte("key"), []byte("val"))
		}
		done <- true
	}()

	// Background flusher
	go func() {
		for range 5 {
			db.Flush()
		}
		done <- true
	}()

	<-done
	<-done

	_, found := db.Get([]byte("key"))
	assert.True(t, found)
}
