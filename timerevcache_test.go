package main

import (
	"testing"
	"time"
)

func TestTimeRevCache(t *testing.T) {

	cacheDuration, _ := time.ParseDuration("5s")
	cache := NewTimeRevCache(cacheDuration)

	// new entry should be OK
	ok := cache.CheckAndUpdate("a", 1)
	if !ok {
		t.Fatalf("key a should be ok")
	}

	// updated entry should be OK 2 > 1
	ok = cache.CheckAndUpdate("a", 2)
	if !ok {
		t.Fatalf("key a should be ok")
	}

	// duplicate entry should be not OK
	ok = cache.CheckAndUpdate("a", 2)
	if ok {
		t.Fatalf("key a should be not ok")
	}

	// allow "a" to expire
	time.Sleep(cacheDuration)

	// 1 should be OK again even though its less than 2
	ok = cache.CheckAndUpdate("a", 1)
	if !ok {
		t.Fatalf("key a should be ok")
	}
}
