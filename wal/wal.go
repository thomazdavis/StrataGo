package wal

import (
	"encoding/binary"
	"os"
	"sync"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

// WriteEntry saves a Key-Value pair to the log.
// Format: [Key Size (4B)] [Value Size (4B)] [Key Bytes] [Value Bytes]
func (w *WAL) WriteEntry(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Calculate sizes
	var buf [8]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(value)))

	// Write the Header
	if _, err := w.file.Write(buf[:]); err != nil {
		return err
	}

	// Write the Key
	if _, err := w.file.Write(key); err != nil {
		return err
	}

	// Write the Value
	if _, err := w.file.Write(value); err != nil {
		return err
	}

	// Sync to Disk: flush the buffer to the hard drive
	return w.file.Sync()
}

// Close safely closes the file handle
func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) Recover() (map[string][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	header := make([]byte, 8)
	data := make(map[string][]byte)

	file, err := os.Open(w.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	for {
		_, err := file.Read(header)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}

		keySize := binary.LittleEndian.Uint32(header[0:4])
		valSize := binary.LittleEndian.Uint32(header[4:8])

		key := make([]byte, keySize)
		_, err = file.Read(key)
		if err != nil {
			return nil, err
		}

		value := make([]byte, valSize)
		_, err = file.Read(value)
		if err != nil {
			return nil, err
		}

		data[string(key)] = value
	}
	return data, nil
}
