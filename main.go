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

	walLog, err := wal.NewWAL("wal.log")
	if err != nil {
		log.Fatalf("CRITICAL: Failed to initialize WAL: %v", err)
	}
	defer walLog.Close()
	fmt.Println("[OK] WAL Initialized (wal.log)")

	memTable := memtable.NewSkipList()
	fmt.Println("[OK] Memtable Initialized")

	// Replay WAL into Memtable
	fmt.Println("[INFO] Recovering data from disk...")
	restoredData, err := walLog.Recover()
	if err != nil {
		log.Fatalf("CRITICAL: WAL Recovery failed: %v", err)
	}

	for key, value := range restoredData {
		memTable.Put([]byte(key), value)
	}
	fmt.Printf("[OK] Recovery Complete. Restored %d keys.\n", len(restoredData))

	// Keep the server running until Ctrl+C
	fmt.Println("-----------------------------")
	fmt.Println("Database is Ready. Press Ctrl+C to exit.")

	// Create a channel to listen for OS signals (Interrupt/Kill)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Block here until signal is received
	<-stop

	fmt.Println("\nShutting down safely...")
	// walLog.Close() runs via defer
}
