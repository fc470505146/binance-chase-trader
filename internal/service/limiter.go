package service

import (
	"context"
	"sync"
	"time"
)

type ReplaceLimiter struct {
	minInterval time.Duration
	mu          sync.Mutex
	nextByKey   map[string]time.Time
}

func NewReplaceLimiter(minInterval time.Duration) *ReplaceLimiter {
	return &ReplaceLimiter{
		minInterval: minInterval,
		nextByKey:   map[string]time.Time{},
	}
}

func (l *ReplaceLimiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	next := l.nextByKey[key]
	if now.Before(next) {
		return false
	}
	l.nextByKey[key] = now.Add(l.minInterval)
	return true
}

type TokenBucket struct {
	capacity int
	tokens   float64
	rate     float64
	last     time.Time
	mu       sync.Mutex
}

func NewTokenBucket(capacity int, refillPerSecond float64) *TokenBucket {
	if capacity <= 0 {
		capacity = 1
	}
	if refillPerSecond <= 0 {
		refillPerSecond = 1
	}
	return &TokenBucket{
		capacity: capacity,
		tokens:   float64(capacity),
		rate:     refillPerSecond,
		last:     time.Now(),
	}
}

func (b *TokenBucket) Wait(ctx context.Context) error {
	for {
		if b.take() {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (b *TokenBucket) Update(capacity int, refillPerSecond float64) {
	if capacity <= 0 || refillPerSecond <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.capacity = capacity
	if b.tokens > float64(capacity) {
		b.tokens = float64(capacity)
	}
	b.rate = refillPerSecond
}

func (b *TokenBucket) take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * b.rate
	if b.tokens > float64(b.capacity) {
		b.tokens = float64(b.capacity)
	}
	b.last = now

	if b.tokens >= 1 {
		b.tokens -= 1
		return true
	}
	return false
}
