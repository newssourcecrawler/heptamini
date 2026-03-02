package transport

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"testing"
	"time"

	"nsc_conn_frame/internal/protocol"
)

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func startTestServer(t *testing.T, maxInFlight uint64, submit func([]byte) ([]byte, error)) (*Server, string) {
	t.Helper()

	cfg := &ServerConfig{
		MaxInFlightBackend: maxInFlight,
		IdleTimeout:        5 * time.Second,
		MaxLineBytes:       256 * 1024,
		MaxJobsPerConn:     16,
		Submit:             submit,
	}

	srv := NewServer("127.0.0.1:0", nil, cfg)

	go func() {
		_ = srv.ListenAndServe()
	}()

	if err := srv.WaitReady(2 * time.Second); err != nil {
		t.Fatalf("server failed to listen: %v", err)
	}

	return srv, srv.Addr()
}

func sendFrame(t *testing.T, conn net.Conn, f *protocol.Frame) {
	t.Helper()
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = conn.Write(append(b, '\n'))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readFrame(t *testing.T, conn net.Conn, r *bufio.Reader) *protocol.Frame {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var f protocol.Frame
	if err := json.Unmarshal(line, &f); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &f
}

func TestPingDegradesWhenSaturated(t *testing.T) {
	// Backend stub: should not be called for ping.
	submit := func(b []byte) ([]byte, error) { return []byte("ok"), nil }

	srv, addr := startTestServer(t, 1, submit)
	t.Cleanup(func() { _ = srv.Close() })

	// Saturate backend capacity directly (deterministic).
	srv.backendSem <- struct{}{}
	t.Cleanup(func() { <-srv.backendSem })

	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	sendFrame(t, conn, &protocol.Frame{
		V:    0,
		Verb: protocol.VerbPing,
		Job:  "Jping",
		Seq:  1,
	})

	r := bufio.NewReader(conn)
	resp := readFrame(t, conn, r)

	got := int64(-1)
	for _, rc := range resp.Receipts {
		if rc.Kind == "server.health" {
			got = int64(rc.ValueU64)
			break
		}
	}
	if got != 0 {
		t.Fatalf("expected degraded health (server.health=0), got %d (receipts=%#v)", got, resp.Receipts)
	}
}

func TestYieldWhenBackendSaturated(t *testing.T) {
	submit := func(b []byte) ([]byte, error) { return []byte("ok"), nil }

	srv, addr := startTestServer(t, 1, submit)
	t.Cleanup(func() { _ = srv.Close() })

	// Saturate backend capacity directly so submit acquisition will fail.
	srv.backendSem <- struct{}{}
	t.Cleanup(func() { <-srv.backendSem })

	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	// Manifest ack
	sendFrame(t, conn, &protocol.Frame{
		V:    0,
		Verb: protocol.VerbShare,
		Job:  "J1",
		Seq:  1,
		PayloadManifest: &protocol.PayloadManifest{
			PayloadID:       "P1",
			ChunkBytes:      8,
			Chunks:          1,
			ContentEncoding: "identity",
			Cipher:          "none",
		},
	})
	_ = readFrame(t, conn, r)

	// Completing chunk should yield immediately due to saturation.
	sendFrame(t, conn, &protocol.Frame{
		V:    0,
		Verb: protocol.VerbShare,
		Job:  "J1",
		Seq:  2,
		PayloadChunk: &protocol.PayloadChunk{
			PayloadID: "P1",
			Index:     0,
			Offset:    0,
			Sha256Hex: shaHex([]byte("x")),
			BytesB64:  "eA==",
		},
	})

	resp := readFrame(t, conn, r)
	if resp.Status != protocol.StatusYield {
		t.Fatalf("expected yield, got %v", resp.Status)
	}
	if resp.RetryHintMs == 0 {
		t.Fatalf("expected retry_hint_ms")
	}
}
