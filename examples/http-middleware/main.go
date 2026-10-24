// Command http-middleware shows the middleware protecting an API with a
// limit shared through Redis: 5 requests per second per client, bursts of
// 10, keyed by API key when present and by client IP otherwise. If Redis
// goes down, a circuit breaker moves decisions to a local in-memory limiter
// until Redis answers again (stop Redis while it runs to see it).
//
//	make up && go run ./examples/http-middleware
//	for i in $(seq 12); do curl -s -o /dev/null -w '%{http_code} ' localhost:8081/hello; done
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/failover"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/memory"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/middleware"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/redisstore"
)

func main() {
	url := os.Getenv("RL_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rdb, err := redisstore.Open(ctx, url)
	if err != nil {
		log.Fatal(err)
	}
	defer rdb.Close()

	local := memory.New(limiter.SystemClock{})
	go func() {
		for range time.Tick(time.Minute) {
			local.Sweep()
		}
	}()
	lim, err := failover.New(redisstore.NewTokenBucket(rdb, ""), failover.Options{
		Fallback: local,
		Replicas: 1, // set to the number of instances sharing this Redis
		Timeout:  100 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}

	ip, err := middleware.ByIP(middleware.IPOptions{})
	if err != nil {
		log.Fatal(err)
	}
	limit, err := middleware.New(middleware.Options{
		Limiter: lim,
		Limit:   limiter.PerSecond(5, 10),
		Key:     middleware.FirstOf(middleware.ByHeader("X-API-Key"), ip),
		Policy:  "hello",
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "hello")
	})

	srv := &http.Server{Addr: ":8081", Handler: limit(mux), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("listening on %s", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}
