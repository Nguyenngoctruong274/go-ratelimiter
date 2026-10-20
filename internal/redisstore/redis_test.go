package redisstore

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOpenRejectsBadURL(t *testing.T) {
	_, err := Open(context.Background(), "http://localhost:6379")
	if err == nil || !strings.Contains(err.Error(), "parse redis url") {
		t.Fatalf("error = %v, want parse error", err)
	}
}

func TestOpenFailsFastWhenRedisIsDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Port 1 is reserved and closed on any sane test machine.
	start := time.Now()
	_, err := Open(ctx, "redis://127.0.0.1:1/0")
	if err == nil || !strings.Contains(err.Error(), "ping redis 127.0.0.1:1") {
		t.Fatalf("error = %v, want ping error", err)
	}
	if time.Since(start) > 4*time.Second {
		t.Fatalf("Open took %s, want a fast failure", time.Since(start))
	}
}

func TestOpenAndPing(t *testing.T) {
	url := os.Getenv("RL_TEST_REDIS_URL")
	if url == "" {
		t.Skip("RL_TEST_REDIS_URL not set; skipping Redis tests")
	}
	ctx := context.Background()
	c, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := Pinger(c)(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
}
