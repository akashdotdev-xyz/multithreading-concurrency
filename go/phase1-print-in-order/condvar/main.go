package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
)

type Foo struct {
	mu    sync.Mutex
	cond  *sync.Cond
	stage int
}

func NewFoo() *Foo {
	f := &Foo{}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *Foo) First(printFirst func()) {
	f.mu.Lock()
	printFirst()
	f.stage = 1
	f.mu.Unlock()
	f.cond.Broadcast()
}

func (f *Foo) Second(printSecond func()) {
	f.mu.Lock()
	for f.stage != 1 {
		f.cond.Wait()
	}
	printSecond()
	f.stage = 2
	f.mu.Unlock()
	f.cond.Broadcast()
}

func (f *Foo) Third(printThird func()) {
	f.mu.Lock()
	for f.stage != 2 {
		f.cond.Wait()
	}
	printThird()
	f.mu.Unlock()
}

func main() {
	foo := NewFoo()

	var mu sync.Mutex
	var order []string
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}

	calls := []func(){
		func() { foo.First(func() { record("first") }) },
		func() { foo.Second(func() { record("second") }) },
		func() { foo.Third(func() { record("third") }) },
	}
	rand.Shuffle(len(calls), func(i, j int) { calls[i], calls[j] = calls[j], calls[i] })

	var wg sync.WaitGroup
	wg.Add(len(calls))
	for _, c := range calls {
		c := c
		go func() { defer wg.Done(); c() }()
	}
	wg.Wait()

	expected := []string{"first", "second", "third"}
	for i := range expected {
		if order[i] != expected[i] {
			fmt.Printf("FAILED: got %v\n", order)
			os.Exit(1)
		}
	}
	fmt.Printf("OK: %v\n", order)
}
