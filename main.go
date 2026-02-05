package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/thomazdavis/flashdb/memtable"
	"github.com/thomazdavis/flashdb/wal"
)

func main() {
	fmt.Println("FlashDB Storage Engine")
	fmt.Println("-----------------------------")

	// Initialize WAL
	walLog, err := wal.NewWAL("wal.log")
	if err != nil {
		log.Fatalf("!! Failed to initialize WAL: %v", err)
	}
	fmt.Println("WAL Initialized (wal.log)")

	// Initialize Memtable
	memTable := memtable.NewSkipList()
	fmt.Println("Memtable Initialized")

	// Replay WAL into Memtable
	fmt.Println("Recovering data from disk...")
	restoredData, err := walLog.Recover()
	if err != nil {
		log.Fatalf("!!WAL Recovery failed: %v", err)
	}
	for key, value := range restoredData {
		memTable.Put([]byte(key), value)
	}
	fmt.Printf("Recovery Complete. Restored %d keys.\n", len(restoredData))

	// Server Loop (Wait for Signal)
	fmt.Println("-----------------------------")
	fmt.Println("Database is Ready. Press Ctrl+C to exit.")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop // Block here until signal received

	fmt.Println("\nShutting down safely...")

	if err := walLog.Close(); err != nil {
		log.Printf("Error closing WAL: %v", err)
	}
}
