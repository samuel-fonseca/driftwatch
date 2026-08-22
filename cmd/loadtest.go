package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	numClients := flag.Int("n", 1000, "number of concurrent subscribers")
	url := flag.String("url", "http://localhost:8080/stream", "SSE endpoint to connect to")
	duration := flag.Duration("duration", 20*time.Second, "how long to hold connections open")
	flag.Parse()

	transport := &http.Transport{
		MaxIdleConns:        *numClients + 100,
		MaxIdleConnsPerHost: *numClients + 100,
		MaxConnsPerHost:     0, // unlimited
	}
	client := &http.Client{Transport: transport}

	ctx, cancel := context.WithTimeout(context.Background(), *duration+10*time.Second)
	defer cancel()

	var (
		connected   atomic.Int64
		failed      atomic.Int64
		totalEvents atomic.Int64
	)

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < *numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, *url, nil)
			if err != nil {
				failed.Add(1)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				failed.Add(1)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				failed.Add(1)
				return
			}
			connected.Add(1)

			reader := bufio.NewReader(resp.Body)
			deadline := time.Now().Add(*duration)
			for time.Now().Before(deadline) {
				line, err := reader.ReadString('\n')
				if err != nil {
					return // connection closed or errored -- stop counting for this client
				}
				if strings.HasPrefix(line, "data:") || strings.HasPrefix(line, ": heartbeat") {
					totalEvents.Add(1)
				}
			}
		}(i)
	}

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("connected=%d failed=%d events_so_far=%d elapsed=%v",
					connected.Load(), failed.Load(), totalEvents.Load(), time.Since(start).Round(time.Second))
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	fmt.Println("--- loadtest summary ---")
	fmt.Printf("requested:      %d\n", *numClients)
	fmt.Printf("connected:      %d\n", connected.Load())
	fmt.Printf("failed:         %d\n", failed.Load())
	fmt.Printf("total events:   %d\n", totalEvents.Load())
	fmt.Printf("wall time:      %v\n", time.Since(start).Round(time.Millisecond))
}
