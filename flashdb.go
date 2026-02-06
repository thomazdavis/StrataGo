package flashdb

import (
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thomazdavis/flashdb/memtable"
	"github.com/thomazdavis/flashdb/sstable"
	"github.com/thomazdavis/flashdb/wal"
)

type FlashDB struct {
	mu             sync.RWMutex
	activeMemtable *memtable.SkipList
	wal            *wal.WAL
	sstReaders     []*sstable.Reader
	dataDir        string
}

func Open(dataDir string) (*FlashDB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dataDir, "wal.log")
	walLog, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	mem := memtable.NewSkipList()

	restored, _ := walLog.Recover()
	for k, v := range restored {
		mem.Put([]byte(k), v)
	}

	files, _ := os.ReadDir(dataDir)
	var readers []*sstable.Reader
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".sst") {
			r, err := sstable.NewReader(filepath.Join(dataDir, f.Name()))
			if err == nil {
				readers = append(readers, r)
			}
		}
	}
	return &FlashDB{
		activeMemtable: mem,
		wal:            walLog,
		sstReaders:     readers,
		dataDir:        dataDir,
	}, nil
}

func (db *FlashDB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.WriteEntry(key, value); err != nil {
		return err
	}

	db.activeMemtable.Put(key, value)
	return nil
}

func (db *FlashDB) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if val, found := db.activeMemtable.Get(key); found {
		return val, true
	}

	for i := len(db.sstReaders) - 1; i >= 0; i-- {
		if val, found := db.sstReaders[i].Get(key); found {
			return val, true
		}
	}
	return nil, false
}

func (db *FlashDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.wal.Close()
	for _, r := range db.sstReaders {
		r.Close()
	}
	return nil
}
