package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
)

type Foo struct {
	secondCh chan struct{}
	thirdCh  chan struct{}
}

func NewFoo() *Foo {
	return &Foo{
		secondCh: make(chan struct{}, 1),
		thirdCh:  make(chan struct{}, 1),
	}
}

func (f *Foo) First(printFirst func()) {
	printFirst()
	f.secondCh <- struct{}{}
}

func (f *Foo) Second(printSecond func()) {
	<-f.secondCh
	printSecond()
	f.thirdCh <- struct{}{}
}

func (f *Foo) Third(printThird func()) {
	<-f.thirdCh
	printThird()
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
