package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type WAL struct {
	file           *os.File
	mu             sync.Mutex
	path           string
	sequenceNumber uint64
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: file, path: path,
	}, nil
}

// WriteEntry saves a Key-Value pair to the log.
// Format: [Key Size (4B)] [Value Size (4B)] [Key Bytes] [Value Bytes]
func (w *WAL) WriteEntry(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sequenceNumber++

	// Calculate checksum over key+value
	h := crc32.NewIEEE()
	h.Write(key)
	h.Write(value)
	checksum := h.Sum32()

	// Calculate sizes
	var buf [20]byte
	binary.LittleEndian.PutUint64(buf[0:8], w.sequenceNumber)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(value)))
	binary.LittleEndian.PutUint32(buf[16:20], checksum)

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
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func (w *WAL) Path() string {
	return w.path
}

func (w *WAL) Recover() (map[string][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	header := make([]byte, 20) // SeqNum(8) + KeySize(4) + ValSize(4) + Checksum(4)
	data := make(map[string][]byte)

	file, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return data, nil
		}
		return nil, err
	}
	defer file.Close()

	for {
		n, err := file.Read(header)
		if err == io.EOF {
			break
		}
		if err != nil || n < 20 {
			// Partial header - likely last write was incomplete due to crash
			// Stop recovery here
			break
		}

		seqNum := binary.LittleEndian.Uint64(header[0:8])
		keySize := binary.LittleEndian.Uint32(header[8:12])
		valSize := binary.LittleEndian.Uint32(header[12:16])
		expectedChecksum := binary.LittleEndian.Uint32(header[16:20])

		key := make([]byte, keySize)
		n, err = file.Read(key)
		if err == io.EOF || n < int(keySize) {
			break
		}
		if err != nil {
			return nil, err
		}

		value := make([]byte, valSize)
		n, err = file.Read(value)
		if err == io.EOF || n < int(valSize) {
			break
		}
		if err != nil {
			return nil, err
		}

		actualChecksum := crc32.ChecksumIEEE(append(key, value...))
		if actualChecksum != expectedChecksum {
			break
		}

		if seqNum > w.sequenceNumber {
			w.sequenceNumber = seqNum
		}

		data[string(key)] = value
	}
	return data, nil
}
