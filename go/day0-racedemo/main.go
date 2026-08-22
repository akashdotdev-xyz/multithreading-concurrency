package main

import (
	"fmt"
	"os"
	"sync"
)

const (
	numGoroutines  = 50
	incrementsEach = 10000
)

// Deliberately unsynchronized: this is the Day 0 harness demo, meant to
// fail intermittently (or under -race) so we can confirm the stress
// scripts actually catch races before relying on them for real problems.
func main() {
	var counter int
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsEach; j++ {
				counter++
			}
		}()
	}
	wg.Wait()

	expected := numGoroutines * incrementsEach
	if counter != expected {
		fmt.Printf("FAILED: expected %d, got %d\n", expected, counter)
		os.Exit(1)
	}
	fmt.Printf("OK: %d\n", counter)
}
