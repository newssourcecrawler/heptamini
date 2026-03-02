package transport

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"nsc_conn_frame/internal/protocol"
)

// ---- Server / accept loop ----

// ErrServerClosed is returned by ListenAndServe when the server is closed intentionally.
var ErrServerClosed = errors.New("server closed")

const (
	defaultRetryHintMs        = 500
	defaultIdleTimeout        = 60 * time.Second
	defaultMaxLineBytes       = 256 * 1024
	defaultMaxJobsPerConn     = 64
	defaultMaxInFlightBackend = 4
)

// Observer is optional and best-effort; it may be nil.
// It exists only to let callers observe frames without pulling in extra packages.
type Observer interface {
	OnFrameIn(remote string, f *protocol.Frame)
	OnFrameOut(remote string, f *protocol.Frame)
}

type ServerConfig struct {
	// MaxInFlightBackend caps concurrent backend submissions.
	MaxInFlightBackend uint64

	// Safety knobs.
	IdleTimeout    time.Duration
	MaxLineBytes   int
	MaxJobsPerConn int

	// Submit, if set, is called once when a job payload is complete.
	// Transport remains compute-agnostic; this is a pure bytes-in/bytes-out hook.
	Submit func(payload []byte) ([]byte, error)
}

func defaultServerConfig() ServerConfig {
	return ServerConfig{
		MaxInFlightBackend: defaultMaxInFlightBackend,
		IdleTimeout:        defaultIdleTimeout,
		MaxLineBytes:       defaultMaxLineBytes,
		MaxJobsPerConn:     defaultMaxJobsPerConn,
	}
}

type Server struct {
	addr string
	obs  Observer

	// Backend arbiter: in-flight submissions (Allow / Yield).
	backendSem chan struct{}

	cfg ServerConfig

	// Counters (best-effort, monotonic). Used for demos/metrics.
	executedTotal uint64
	yieldedTotal  uint64

	ready     chan struct{}
	readyOnce sync.Once
	listenErr error

	ln        net.Listener
	quit      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	submit func(payload []byte) ([]byte, error)
}

func (s *Server) incExecuted() {
	atomic.AddUint64(&s.executedTotal, 1)
}

func (s *Server) incYielded() {
	atomic.AddUint64(&s.yieldedTotal, 1)
}

func NewServer(addr string, obs Observer, cfg *ServerConfig) *Server {
	c := defaultServerConfig()
	if cfg != nil {
		if cfg.MaxInFlightBackend != 0 {
			c.MaxInFlightBackend = cfg.MaxInFlightBackend
		}
		if cfg.IdleTimeout != 0 {
			c.IdleTimeout = cfg.IdleTimeout
		}
		if cfg.MaxLineBytes != 0 {
			c.MaxLineBytes = cfg.MaxLineBytes
		}
		if cfg.MaxJobsPerConn != 0 {
			c.MaxJobsPerConn = cfg.MaxJobsPerConn
		}
	}

	var submit func([]byte) ([]byte, error)
	if cfg != nil {
		submit = cfg.Submit
	}

	return &Server{
		addr:       addr,
		obs:        obs,
		quit:       make(chan struct{}),
		cfg:        c,
		submit:     submit,
		backendSem: make(chan struct{}, int(c.MaxInFlightBackend)),
		ready:      make(chan struct{}),
	}
}

// signalReady closes the ready channel exactly once and records an optional listen error.
func (s *Server) signalReady(err error) {
	if err != nil {
		s.listenErr = err
	}
	s.readyOnce.Do(func() {
		close(s.ready)
	})
}

// Addr returns the bound listen address once ready. If not yet listening, returns the configured addr.
func (s *Server) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

// WaitReady blocks until the server has either successfully bound its listener or failed.
// It returns any listen error.
func (s *Server) WaitReady(timeout time.Duration) error {
	select {
	case <-s.ready:
		return s.listenErr
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for listen")
	}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.signalReady(err)
		return err
	}
	s.ln = ln
	s.signalReady(nil)
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return ErrServerClosed
			default:
				return err
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			_ = s.handleConn(conn)
		}()
	}
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.signalReady(ErrServerClosed)
		close(s.quit)
		if s.ln != nil {
			_ = s.ln.Close()
		}
	})
	s.wg.Wait()
	return nil
}

// ---- Connection loop: upload protocol + backend cap ----

func (s *Server) healthU64() uint64 {
	capInFlight := cap(s.backendSem)
	if capInFlight <= 0 {
		return 1
	}
	if len(s.backendSem) >= capInFlight {
		return 0
	}
	return 1
}

func (s *Server) coreReceipts(bytesIn uint64) []protocol.Receipt {
	return []protocol.Receipt{
		{Kind: "server.health", ValueU64: s.healthU64()},
		{Kind: "backend.inflight", ValueU64: uint64(len(s.backendSem))},
		{Kind: "backend.cap", ValueU64: uint64(cap(s.backendSem))},
		{Kind: "server.executed_total", ValueU64: atomic.LoadUint64(&s.executedTotal)},
		{Kind: "server.yielded_total", ValueU64: atomic.LoadUint64(&s.yieldedTotal)},
		{Kind: "cdr.bytes_in", ValueU64: bytesIn},
	}
}

// MetricsText returns Prometheus-style exposition text.
// Privacy: exports only governor/accounting counters and gauges.
func (s *Server) MetricsText() string {
	var b bytes.Buffer

	// Gauges
	b.WriteString("# TYPE heptamini_backend_inflight gauge\n")
	b.WriteString("heptamini_backend_inflight ")
	b.WriteString(strconv.FormatUint(uint64(len(s.backendSem)), 10))
	b.WriteString("\n")

	b.WriteString("# TYPE heptamini_backend_cap gauge\n")
	b.WriteString("heptamini_backend_cap ")
	b.WriteString(strconv.FormatUint(uint64(cap(s.backendSem)), 10))
	b.WriteString("\n")

	b.WriteString("# TYPE heptamini_server_health gauge\n")
	b.WriteString("heptamini_server_health ")
	b.WriteString(strconv.FormatUint(s.healthU64(), 10))
	b.WriteString("\n")

	// Counters
	b.WriteString("# TYPE heptamini_executed_total counter\n")
	b.WriteString("heptamini_executed_total ")
	b.WriteString(strconv.FormatUint(atomic.LoadUint64(&s.executedTotal), 10))
	b.WriteString("\n")

	b.WriteString("# TYPE heptamini_yielded_total counter\n")
	b.WriteString("heptamini_yielded_total ")
	b.WriteString(strconv.FormatUint(atomic.LoadUint64(&s.yieldedTotal), 10))
	b.WriteString("\n")

	return b.String()
}

func (s *Server) handleConn(conn net.Conn) error {
	defer conn.Close()
	remote := conn.RemoteAddr().String()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	defer w.Flush()

	// v1: per-connection job table (in-memory).
	jobs := map[string]*protocol.UploadState{}

	// Helper to zeroize and drop per-job upload buffers for privacy hardening.
	scrubAndDrop := func(job string, u *protocol.UploadState) {
		if u == nil {
			delete(jobs, job)
			return
		}
		// Best-effort zeroization (Go cannot guarantee, but this prevents accidental retention/logging).
		for i := range u.Buf {
			u.Buf[i] = 0
		}
		u.Buf = nil
		u.Manifest = nil
		delete(jobs, job)
	}

	// Once we send any terminal status on this connection, do not emit an additional peer_gone abort on EOF.
	var terminalSent bool

	// Track the most recent valid envelope so terminal errors can reference a real job/seq when possible.
	lastJob := "J_?"
	var lastSeq uint64 = 0
	var lastTTLms uint64 = 0

	send := func(f *protocol.Frame) error {
		if f == nil {
			return nil
		}

		if f.Status == protocol.StatusYield || f.Status == protocol.StatusAbort || f.Status == protocol.StatusDone {
			terminalSent = true
		}

		if s.obs != nil {
			s.obs.OnFrameOut(remote, f)
		}

		b, err := json.Marshal(f)
		if err != nil {
			return err
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			return err
		}
		return w.Flush()
	}

	handleManifest := func(f *protocol.Frame, u *protocol.UploadState) {
		if err := u.ApplyManifest(f.PayloadManifest); err != nil {
			_ = send(&protocol.Frame{
				V:         0,
				Verb:      protocol.VerbClarify,
				Job:       f.Job,
				Seq:       f.Seq + 1,
				TTLms:     f.TTLms,
				Status:    protocol.StatusNeed,
				NeedCode:  "manifest_invalid",
				ErrorCode: "need_manifest_fix",
				Receipts: append(s.coreReceipts(0),
					protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 3}, // invalid_manifest
				),
			})
			return
		}

		_ = send(&protocol.Frame{
			V:        0,
			Verb:     protocol.VerbClarify,
			Job:      f.Job,
			Seq:      f.Seq + 1,
			TTLms:    f.TTLms,
			Status:   protocol.StatusOK,
			Receipts: s.coreReceipts(0),
		})
	}

	handleChunk := func(f *protocol.Frame, u *protocol.UploadState) {
		need, err := u.ApplyChunk(f.PayloadChunk)
		if err != nil {
			_ = send(&protocol.Frame{
				V:         0,
				Verb:      protocol.VerbClarify,
				Job:       f.Job,
				Seq:       f.Seq + 1,
				TTLms:     f.TTLms,
				Status:    protocol.StatusNeed,
				NeedCode:  "chunk_apply_error",
				ErrorCode: "need_chunk_fix",
				Receipts: append(s.coreReceipts(uint64(len(u.Buf))),
					protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 4}, // invalid_chunk
				),
			})
			return
		}
		if need != nil {
			_ = send(&protocol.Frame{
				V:         0,
				Verb:      protocol.VerbClarify,
				Job:       f.Job,
				Seq:       f.Seq + 1,
				TTLms:     f.TTLms,
				Status:    protocol.StatusNeed,
				NeedCode:  need.Code,
				ErrorCode: "need_more",
				Receipts:  s.coreReceipts(uint64(len(u.Buf))),
			})
			return
		}

		// Successful chunk applied and upload still in progress: send an explicit ack.
		// (Clients expect one reply per chunk; no payload is echoed.)
		if !u.IsComplete() {
			_ = send(&protocol.Frame{
				V:        0,
				Verb:     protocol.VerbClarify,
				Job:      f.Job,
				Seq:      f.Seq + 1,
				TTLms:    f.TTLms,
				Status:   protocol.StatusOK,
				Receipts: s.coreReceipts(uint64(len(u.Buf))),
			})
			return
		}

		// Payload complete: optionally hand off to a backend runner.
		if s.submit == nil {
			_ = send(&protocol.Frame{
				V:      0,
				Verb:   protocol.VerbConfirm,
				Job:    f.Job,
				Seq:    f.Seq + 2,
				TTLms:  f.TTLms,
				Status: protocol.StatusDone,
				Receipts: append(s.coreReceipts(uint64(len(u.Buf))),
					protocol.Receipt{Kind: "conn.outcome", ValueU64: 1},
				),
			})
			scrubAndDrop(f.Job, u)
			return
		}

		// Backend arbiter: Allow / Yield.
		select {
		case s.backendSem <- struct{}{}:
			// acquired
			s.incExecuted()
		default:
			s.incYielded()
			hint := uint64(defaultRetryHintMs)
			_ = send(&protocol.Frame{
				V:           0,
				Verb:        protocol.VerbClarify,
				Job:         f.Job,
				Seq:         f.Seq + 2,
				TTLms:       f.TTLms,
				Status:      protocol.StatusYield,
				NeedCode:    "server_busy",
				ErrorCode:   "busy",
				RetryHintMs: hint,
				Receipts: append(s.coreReceipts(uint64(len(u.Buf))),
					protocol.Receipt{Kind: "conn.outcome", ValueU64: 3},
					protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 1},
					protocol.Receipt{Kind: "conn.retry_hint_ms", ValueU64: hint},
				),
			})
			return
		}
		defer func() { <-s.backendSem }()

		t0 := time.Now()
		_, berr := s.submit(u.Buf)
		backendMs := uint64(time.Since(t0) / time.Millisecond)
		if berr != nil {
			_ = send(&protocol.Frame{
				V:         0,
				Verb:      protocol.VerbConfirm,
				Job:       f.Job,
				Seq:       f.Seq + 2,
				TTLms:     f.TTLms,
				Status:    protocol.StatusAbort,
				NeedCode:  "backend_error",
				ErrorCode: "backend_error",
				Receipts: append(s.coreReceipts(uint64(len(u.Buf))),
					protocol.Receipt{Kind: "cdr.backend.ms", ValueU64: backendMs},
					protocol.Receipt{Kind: "conn.outcome", ValueU64: 2},
					protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 8},
				),
			})
			scrubAndDrop(f.Job, u)
			return
		}

		_ = send(&protocol.Frame{
			V:      0,
			Verb:   protocol.VerbConfirm,
			Job:    f.Job,
			Seq:    f.Seq + 2,
			TTLms:  f.TTLms,
			Status: protocol.StatusDone,
			Receipts: append(s.coreReceipts(uint64(len(u.Buf))),
				protocol.Receipt{Kind: "cdr.backend.ms", ValueU64: backendMs},
				protocol.Receipt{Kind: "conn.outcome", ValueU64: 1},
			),
		})
		scrubAndDrop(f.Job, u)
	}

	for {
		now := time.Now()
		_ = conn.SetReadDeadline(now.Add(s.cfg.IdleTimeout))
		line, err := readLineLimited(r, s.cfg.MaxLineBytes)
		if err != nil {
			if err.Error() == "line_too_large" {
				_ = send(&protocol.Frame{
					V:         0,
					Verb:      protocol.VerbConfirm,
					Job:       lastJob,
					Seq:       lastSeq + 1,
					TTLms:     lastTTLms,
					Status:    protocol.StatusAbort,
					NeedCode:  "line_too_large",
					ErrorCode: "bad_frame",
					Receipts: append(s.coreReceipts(0),
						protocol.Receipt{Kind: "conn.outcome", ValueU64: 2},
						protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 2},
					),
				})
				return err
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				_ = send(&protocol.Frame{
					V:         0,
					Verb:      protocol.VerbConfirm,
					Job:       lastJob,
					Seq:       lastSeq + 1,
					TTLms:     lastTTLms,
					Status:    protocol.StatusAbort,
					NeedCode:  "idle_timeout",
					ErrorCode: "timeout",
					Receipts: append(s.coreReceipts(0),
						protocol.Receipt{Kind: "conn.outcome", ValueU64: 4},
						protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 9},
					),
				})
				return err
			}

			// Non-timeout read error (often client disconnect). Account as peer_gone (best-effort).
			if terminalSent {
				return err
			}
			_ = send(&protocol.Frame{
				V:         0,
				Verb:      protocol.VerbConfirm,
				Job:       lastJob,
				Seq:       lastSeq + 1,
				TTLms:     lastTTLms,
				Status:    protocol.StatusAbort,
				NeedCode:  "peer_gone",
				ErrorCode: "conn_closed",
				Receipts: append(s.coreReceipts(0),
					protocol.Receipt{Kind: "conn.outcome", ValueU64: 5},
					protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 7},
				),
			})
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var f protocol.Frame
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			needCode, _ := classifyBadLine(line)
			_ = send(&protocol.Frame{
				V:         0,
				Verb:      protocol.VerbClarify,
				Job:       lastJob,
				Seq:       lastSeq + 1,
				TTLms:     lastTTLms,
				Status:    protocol.StatusNeed,
				NeedCode:  needCode,
				ErrorCode: "bad_frame",
				Receipts: append(s.coreReceipts(0),
					protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 2},
				),
			})
			continue
		}

		if err := protocol.ValidateEnvelope(&f); err != nil {
			_ = send(&protocol.Frame{
				V:         0,
				Verb:      protocol.VerbClarify,
				Job:       safeJob(&f),
				Seq:       f.Seq + 1,
				TTLms:     f.TTLms,
				Status:    protocol.StatusNeed,
				NeedCode:  "envelope_invalid",
				ErrorCode: "bad_envelope",
				Receipts: append(s.coreReceipts(0),
					protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 2},
				),
			})
			continue
		}

		lastJob = safeJob(&f)
		lastSeq = f.Seq
		lastTTLms = f.TTLms

		if s.obs != nil {
			s.obs.OnFrameIn(remote, &f)
		}

		// Health/liveness probe. No backend polling.
		if f.Verb == protocol.VerbPing {
			s.handlePing(send, &f)
			continue
		}

		// Ensure upload state exists per job
		u, ok := jobs[f.Job]
		if !ok {
			if len(jobs) >= s.cfg.MaxJobsPerConn {
				_ = send(&protocol.Frame{
					V:         0,
					Verb:      protocol.VerbConfirm,
					Job:       f.Job,
					Seq:       f.Seq + 1,
					TTLms:     f.TTLms,
					Status:    protocol.StatusAbort,
					NeedCode:  "too_many_jobs",
					ErrorCode: "quota",
					Receipts: append(s.coreReceipts(0),
						protocol.Receipt{Kind: "conn.outcome", ValueU64: 2},
						protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 5},
					),
				})
				return fmt.Errorf("too_many_jobs")
			}
			u = protocol.NewUploadState(f.Job)
			jobs[f.Job] = u
		}

		if f.PayloadManifest != nil {
			handleManifest(&f, u)
			continue
		}

		if f.PayloadChunk != nil {
			handleChunk(&f, u)
			continue
		}

		// Unknown message: ask for clarification.
		_ = send(&protocol.Frame{
			V:         0,
			Verb:      protocol.VerbClarify,
			Job:       f.Job,
			Seq:       f.Seq + 1,
			TTLms:     f.TTLms,
			Status:    protocol.StatusNeed,
			NeedCode:  "missing_payload",
			ErrorCode: "need_manifest_or_chunk",
			Receipts: append(s.coreReceipts(0),
				protocol.Receipt{Kind: "conn.outcome_reason", ValueU64: 2},
			),
		})
	}
}

// ---- Health / ping ----

func (s *Server) handlePing(send func(*protocol.Frame) error, in *protocol.Frame) {
	// No backend polling. Report only local/governor state.
	healthU64 := uint64(1)
	capInFlight := uint64(0)
	inFlight := uint64(0)
	if s.backendSem != nil {
		capInFlight = uint64(cap(s.backendSem))
		inFlight = uint64(len(s.backendSem))
	}
	// Degrade when backend is saturated.
	if capInFlight > 0 && inFlight >= capInFlight {
		healthU64 = 0
	}

	receipts := s.coreReceipts(0)
	// Override server.health to reflect ping's degraded semantics.
	for i := range receipts {
		if receipts[i].Kind == "server.health" {
			receipts[i].ValueU64 = healthU64
			break
		}
	}

	_ = send(&protocol.Frame{
		V:        0,
		Verb:     protocol.VerbConfirm,
		Job:      safeJob(in),
		Seq:      in.Seq + 1,
		TTLms:    in.TTLms,
		Status:   protocol.StatusOK,
		Receipts: receipts,
	})
}

// ---- Line IO helpers ----

// readLineLimited reads a line up to maxBytes, returning error "line_too_large" if exceeded.
func readLineLimited(r *bufio.Reader, maxBytes int) (string, error) {
	// Read up to and including '\n'. If the line exceeds maxBytes without a newline, treat as too large.
	var b []byte
	for {
		frag, isPrefix, err := r.ReadLine()
		if err != nil {
			return "", err
		}
		b = append(b, frag...)
		// ReadLine strips the end-of-line; we treat !isPrefix as end of line.
		if len(b) > maxBytes {
			return "", fmt.Errorf("line_too_large")
		}
		if !isPrefix {
			return string(b), nil
		}
	}
}

// ---- Small helpers ----

func safeJob(f *protocol.Frame) string {
	if f == nil || f.Job == "" {
		return "J_?"
	}
	return f.Job
}

// classifyBadLine returns a stable need code + human message for common non-frame inputs.
func classifyBadLine(line string) (needCode string, msg string) {
	l := strings.TrimSpace(line)
	if l == "" {
		return "empty_line", "empty line"
	}
	// Common accidental protocol mismatches.
	if strings.HasPrefix(l, "GET ") || strings.HasPrefix(l, "POST ") || strings.HasPrefix(l, "PUT ") || strings.HasPrefix(l, "HEAD ") {
		return "http_request", "looks like an HTTP request; this port expects JSON line frames"
	}
	if strings.HasPrefix(l, "GIF87a") || strings.HasPrefix(l, "GIF89a") {
		return "binary_gif", "looks like a GIF header; this port expects JSON line frames"
	}
	if strings.HasPrefix(l, "<!DOCTYPE") || strings.HasPrefix(l, "<html") || strings.HasPrefix(l, "<HTML") {
		return "html_document", "looks like HTML; this port expects JSON line frames"
	}
	if strings.HasPrefix(l, "<?xml") || strings.HasPrefix(l, "<rss") || strings.HasPrefix(l, "<feed") {
		return "xml_document", "looks like XML; this port expects JSON line frames"
	}
	// JSON-ish but not a valid frame.
	if strings.HasPrefix(l, "[") {
		return "json_array", "looks like a JSON array; expected a single JSON object frame per line"
	}
	if strings.HasPrefix(l, "{") {
		return "json_malformed", "looks like JSON but could not parse a frame object"
	}
	return "not_json", "line is not valid JSON frame"
}
