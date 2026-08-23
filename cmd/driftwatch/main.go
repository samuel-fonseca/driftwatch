package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/samuel-fonseca/driftwatch/internal/hub"
	"github.com/samuel-fonseca/driftwatch/internal/pipeline"
	"github.com/samuel-fonseca/driftwatch/internal/source"
	"github.com/samuel-fonseca/driftwatch/internal/source/binance"
	"github.com/samuel-fonseca/driftwatch/internal/source/bitfinex"
	"github.com/samuel-fonseca/driftwatch/internal/store/psql"
)

func loadEnvironmentVariables() {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatalf("load failed: %v", err)
	}
}

func main() {
	loadEnvironmentVariables()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := psql.Open(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("open failed: %v", err)
	}
	defer st.Close()

	h := hub.New()
	p := pipeline.New(pipeline.Config{
		Sources: []source.Source{
			binance.New(),
			bitfinex.New(),
		},
		Store: st,
		Hub:   h,
	})

	mux := http.NewServeMux()
	mux.Handle("/stream", h)
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p.Stats())
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := p.Run(ctx); err != nil {
			log.Printf("pipe stopped: %v", err)
		}
	}()

	go func() {
		log.Println("listening on :8080 (GET /stream for signals, /stats for pipeline stats)")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	log.Println("shutdown complete")
}
