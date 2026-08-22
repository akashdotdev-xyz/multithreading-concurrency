package main

import (
	"fmt"
	"os"
	"sync"
)

type FooBar struct {
	n      int
	evenCh chan struct{}
	oddCh  chan struct{}
}

func NewFooBar(n int) *FooBar {
	f := &FooBar{
		n:      n,
		evenCh: make(chan struct{}, 1),
		oddCh:  make(chan struct{}, 1),
	}
	f.evenCh <- struct{}{}
	return f
}

func (fb *FooBar) Foo(printFoo func()) {
	for i := 0; i < fb.n; i++ {
		<-fb.evenCh
		printFoo()
		fb.oddCh <- struct{}{}
	}
}

func (fb *FooBar) Bar(printBar func()) {
	for i := 0; i < fb.n; i++ {
		<-fb.oddCh
		printBar()
		fb.evenCh <- struct{}{}
	}
}

func main() {
	const n = 2000
	fb := NewFooBar(n)

	var mu sync.Mutex
	var order []string
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); fb.Foo(func() { record("foo") }) }()
	go func() { defer wg.Done(); fb.Bar(func() { record("bar") }) }()
	wg.Wait()

	if len(order) != 2*n {
		fmt.Printf("FAILED: expected %d entries, got %d\n", 2*n, len(order))
		os.Exit(1)
	}
	for i := 0; i < n; i++ {
		if order[2*i] != "foo" || order[2*i+1] != "bar" {
			fmt.Printf("FAILED at round %d: got %v\n", i, order[2*i:2*i+2])
			os.Exit(1)
		}
	}
	fmt.Printf("OK: %d rounds alternated correctly\n", n)
}
