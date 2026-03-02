package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"nsc_conn_frame/internal/transport"
)

// v1 ships with a stub backend only. Adapters (e.g. HTTP) are v1.1.
type stubBackend struct{ Sleep time.Duration }

func (b stubBackend) Submit(payload []byte) ([]byte, error) {
	if b.Sleep > 0 {
		time.Sleep(b.Sleep)
	}
	// Privacy: do not return output bytes.
	return nil, nil
}

func main() {
	var listen string
	var backendAddr string
	var maxInFlight uint
	var idleTimeout time.Duration
	var maxLineBytes int
	var maxJobsPerConn int
	var metricsListen string
	var stubMs int

	flag.StringVar(&listen, "listen", "0.0.0.0:7777", "TCP listen address (JSON lines)")
	flag.StringVar(&backendAddr, "backend", "", "Optional backend address (v1 uses stub backend regardless; adapters are v1.1)")
	flag.UintVar(&maxInFlight, "max_inflight", 4, "Hard cap for concurrent backend submissions")
	flag.DurationVar(&idleTimeout, "idle_timeout", 60*time.Second, "Idle timeout waiting for next frame")
	flag.IntVar(&maxLineBytes, "max_line_bytes", 256*1024, "Maximum bytes per incoming line/frame")
	flag.IntVar(&maxJobsPerConn, "max_jobs_per_conn", 64, "Maximum jobs tracked per connection")
	flag.IntVar(&stubMs, "stub_ms", 0, "If >0, use a local stub backend that sleeps this many ms per submission")
	flag.StringVar(&metricsListen, "metrics_listen", "", "Optional HTTP listen addr for /metrics (e.g. 127.0.0.1:9090)")
	flag.Parse()

	log.SetFlags(0)
	log.SetOutput(os.Stderr)
	log.Printf("heptaminid: listen=%s", listen)

	if backendAddr != "" {
		log.Printf("heptaminid: backend=%s (ignored in v1; using stub)", backendAddr)
	} else {
		log.Printf("heptaminid: backend=stub")
	}
	backend := stubBackend{Sleep: time.Duration(stubMs) * time.Millisecond}

	cfg := transport.ServerConfig{
		MaxInFlightBackend: uint64(maxInFlight),
		IdleTimeout:        idleTimeout,
		MaxLineBytes:       maxLineBytes,
		MaxJobsPerConn:     maxJobsPerConn,
		Submit:             backend.Submit,
	}
	srv := transport.NewServer(listen, nil, &cfg)

	if metricsListen != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = w.Write([]byte(srv.MetricsText()))
		})
		h := &http.Server{Addr: metricsListen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := h.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("heptaminid: metrics server error: %v", err)
			}
		}()
		log.Printf("heptaminid: metrics_listen=%s", metricsListen)
	}

	// Shutdown on SIGINT/SIGTERM.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigc
		log.Printf("heptaminid: shutting down")
		_ = srv.Close()
	}()

	if err := srv.ListenAndServe(); err != nil {
		// Normal shutdown path.
		if errors.Is(err, transport.ErrServerClosed) {
			return
		}
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
