package main

import (
	"time"
)

type TimeRevCache struct {
	expiration time.Duration
	revs       map[string]int
	times      map[string]time.Time
}

func NewTimeRevCache(expiration time.Duration) *TimeRevCache {
	return &TimeRevCache{
		expiration: expiration,
		revs:       make(map[string]int),
		times:      make(map[string]time.Time),
	}
}

func (t TimeRevCache) CheckAndUpdate(key string, rev int) bool {
	currentRev, existsAndNotExpired := t.get(key)
	if existsAndNotExpired {
		if rev <= currentRev {
			return false
		}
		t.set(key, rev)
		return true
	}
	t.set(key, rev)
	return true
}

func (t TimeRevCache) set(key string, rev int) {
	delete(t.revs, key)
	delete(t.times, key)
	t.revs[key] = rev
	t.times[key] = time.Now()
}

func (t TimeRevCache) get(key string) (int, bool) {
	currentRev, exists := t.revs[key]
	if exists {
		// check expiration
		createdTime, exists := t.times[key]
		if exists {
			lifetime := time.Since(createdTime)
			if lifetime <= t.expiration {
				return currentRev, true
			}
			delete(t.revs, key)
			delete(t.times, key)
			return 0, false
		}
		delete(t.revs, key)
		return 0, false
	}
	return 0, false
}
