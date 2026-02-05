package sstable

import (
	"encoding/binary"
	"os"

	"github.com/thomazdavis/flashdb/memtable"
)

type Builder struct {
	file *os.File
}

func NewBuilder(filename string) (*Builder, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &Builder{file: file}, nil
}

// Flush writes the entire Skiplist to the SSTable file
func (b *Builder) Flush(skiplist *memtable.SkipList) error {
	defer b.file.Close()

	iter := skiplist.NewIterator()

	// Iterate through every node
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()

		// Write Key Size (4 bytes)
		if err := binary.Write(b.file, binary.LittleEndian, uint32(len(key))); err != nil {
			return err
		}

		// Write Value Size (4 bytes)
		if err := binary.Write(b.file, binary.LittleEndian, uint32(len(val))); err != nil {
			return err
		}

		// Write Key Bytes
		if _, err := b.file.Write(key); err != nil {
			return err
		}

		if _, err := b.file.Write(val); err != nil {
			return err
		}
	}
	return b.file.Sync()
}
