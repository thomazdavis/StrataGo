package stratago

import (
	"os"
	"testing"
	"time"

	"github.com/thomazdavis/stratago/memtable"
)

func TestGetTier(t *testing.T) {
	mb := int64(1024 * 1024)

	tests := []struct {
		size     int64
		expected int
	}{
		{5 * mb, 0},    // < 10MB
		{15 * mb, 1},   // 10 - 50MB
		{100 * mb, 2},  // 50 - 250MB
		{500 * mb, 3},  // 250MB - 1GB
		{2000 * mb, 4}, // > 1GB
	}

	for _, tc := range tests {
		if tier := getTier(tc.size); tier != tc.expected {
			t.Errorf("For size %d, expected tier %d, got %d", tc.size, tc.expected, tier)
		}
	}
}

func TestRunCompaction_Success(t *testing.T) {
	// Temporary database
	dbDir := "test_compaction_data"
	os.MkdirAll(dbDir, 0755)
	defer os.RemoveAll(dbDir)

	db, err := Open(dbDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Artificially trigger flushes to create exactly 4 SSTables (Tier 0)
	// We put a unique key in each one to verify they all survive the merge
	db.Put([]byte("key1"), []byte("val1"))
	flushActiveMemtableToDisk(db)

	db.Put([]byte("key2"), []byte("val2"))
	flushActiveMemtableToDisk(db)

	db.Put([]byte("key3"), []byte("val3"))
	flushActiveMemtableToDisk(db)

	db.Put([]byte("key4"), []byte("val4"))
	flushActiveMemtableToDisk(db)

	db.mu.RLock()
	readerCount := len(db.sstReaders)
	db.mu.RUnlock()
	if readerCount != CompactionThreshold {
		t.Fatalf("Expected %d readers before compaction, got %d", CompactionThreshold, readerCount)
	}

	// Run Compaction Manually
	err = db.RunCompaction()
	if err != nil {
		t.Fatalf("RunCompaction failed: %v", err)
	}

	// Verify Atomic Swap (Should now be exactly 1 reader)
	db.mu.RLock()
	newReaderCount := len(db.sstReaders)
	db.mu.RUnlock()

	if newReaderCount != 1 {
		t.Fatalf("Expected 1 reader after compaction, got %d", newReaderCount)
	}

	// Verify Data Integrity (Make sure no data was lost in the swap)
	val, found := db.Get([]byte("key1"))
	if !found || string(val) != "val1" {
		t.Errorf("Lost key1 during compaction")
	}
	val, found = db.Get([]byte("key4"))
	if !found || string(val) != "val4" {
		t.Errorf("Lost key4 during compaction")
	}
}

// Helper to simulate a flush for the test
func flushActiveMemtableToDisk(db *StrataGo) {
	db.mu.Lock()
	db.immutableMemtable = db.activeMemtable
	db.activeMemtable = memtable.NewSkipList()
	db.mu.Unlock()

	// Force the flush worker to process it immediately
	db.flushChan <- struct{}{}
	// Sleep briefly to let the goroutine finish writing, instead of chan
	time.Sleep(50 * time.Millisecond)
}
