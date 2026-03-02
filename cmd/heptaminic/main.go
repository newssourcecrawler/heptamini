package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"nsc_conn_frame/internal/protocol"
)

func main() {
	var addr string
	var filePath string
	var chunkBytes int
	var timeoutSec int
	var maxBytes int64
	var maxRetries int
	var maxRetryMs int
	var pingOnly bool
	var csv bool
	var csvHeader bool
	var quiet bool

	flag.StringVar(&addr, "addr", "127.0.0.1:7777", "server address host:port")
	flag.StringVar(&filePath, "file", "", "path to payload file")
	flag.IntVar(&chunkBytes, "chunk", 8192, "chunk size bytes")
	flag.IntVar(&timeoutSec, "timeout_s", 20, "dial+io timeout seconds")
	flag.Int64Var(&maxBytes, "max_bytes", 50*1024*1024, "max input file size in bytes")
	flag.IntVar(&maxRetries, "max_retries", 50, "max retries on yield")
	flag.IntVar(&maxRetryMs, "max_retry_ms", 5000, "cap retry sleep ms")
	flag.BoolVar(&pingOnly, "ping", false, "ping server health and exit")
	flag.BoolVar(&csv, "csv", false, "emit one CSV line per received frame (stable for scripts)")
	flag.BoolVar(&csvHeader, "csv_header", false, "when --csv is set, print CSV header first")
	flag.BoolVar(&quiet, "quiet", false, "suppress human summary prints (only affects non-CSV mode)")
	flag.Parse()

	if csv && csvHeader {
		fmt.Println("ts_unix_ms,verb,job,seq,status,need_code,retry_hint_ms,error")
	}

	if pingOnly {
		conn, err := net.DialTimeout("tcp", addr, time.Duration(timeoutSec)*time.Second)
		if err != nil {
			fmt.Fprintf(os.Stderr, "heptaminic: dial %s: %v\n", addr, err)
			os.Exit(1)
		}
		defer conn.Close()

		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)

		frame := protocol.Frame{
			V:     0,
			Verb:  protocol.VerbPing,
			Job:   "J_ping",
			Seq:   1,
			TTLms: 0,
		}

		if err := writeFrameLine(conn, w, &frame, timeoutSec); err != nil {
			fmt.Fprintf(os.Stderr, "heptaminic: write ping: %v\n", err)
			os.Exit(1)
		}

		resp, err := readFrameLine(conn, r, timeoutSec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "heptaminic: read ping response: %v\n", err)
			os.Exit(1)
		}

		printFrame(resp, csv, quiet)

		// Semantic health: prefer deterministic receipt, fall back to payload.
		health := int64(-1)
		for _, rc := range resp.Receipts {
			if rc.Kind == "server.health" {
				health = int64(rc.ValueU64)
				break
			}
		}

		// Exit codes:
		// 0 = healthy/ready
		// 2 = degraded/unready
		// 1 = protocol/unknown
		if resp.Status != protocol.StatusOK {
			os.Exit(1)
		}
		if health == 1 {
			os.Exit(0)
		}
		if health == 0 {
			os.Exit(2)
		}
		os.Exit(1)
	}

	if !pingOnly && filePath == "" {
		fmt.Fprintln(os.Stderr, "heptaminic: missing --file")
		os.Exit(1)
	}
	if chunkBytes <= 0 {
		fmt.Fprintln(os.Stderr, "heptaminic: --chunk must be > 0")
		os.Exit(1)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "heptaminic: file does not exist: %s\n", filePath)
		} else {
			fmt.Fprintf(os.Stderr, "heptaminic: cannot access file %s: %v\n", filePath, err)
		}
		os.Exit(1)
	}
	if info.IsDir() {
		fmt.Fprintf(os.Stderr, "heptaminic: path is a directory, not a file: %s\n", filePath)
		os.Exit(1)
	}

	if maxBytes > 0 && info.Size() > maxBytes {
		fmt.Fprintf(os.Stderr, "heptaminic: file too large: %d bytes (max %d): %s\n", info.Size(), maxBytes, filePath)
		os.Exit(1)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "heptaminic: failed reading file %s: %v\n", filePath, err)
		os.Exit(1)
	}

	job := deriveJob(filePath)
	payloadID := "P_1"

	totalHash := sha256.Sum256(data)
	totalHashHex := hex.EncodeToString(totalHash[:])

	chunks := splitChunks(data, chunkBytes)

	conn, err := net.DialTimeout("tcp", addr, time.Duration(timeoutSec)*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "heptaminic: dial %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	seq := uint64(1)

	// 1) share manifest
	manifest := protocol.Frame{
		V:     0,
		Verb:  protocol.VerbShare,
		Job:   job,
		Seq:   seq,
		TTLms: 0,
		PayloadManifest: &protocol.PayloadManifest{
			PayloadID:        payloadID,
			ContentType:      "application/octet-stream",
			ContentEncoding:  "identity",
			Cipher:           "none",
			TotalBytes:       uint64(len(data)),
			ChunkBytes:       uint64(chunkBytes),
			Chunks:           uint64(len(chunks)),
			Sha256HexEncoded: totalHashHex,
		},
	}
	seq++

	if err := writeFrameLine(conn, w, &manifest, timeoutSec); err != nil {
		fmt.Fprintf(os.Stderr, "heptaminic: write manifest: %v\n", err)
		os.Exit(1)
	}

	// read & print responses for manifest; handle yield/backpressure by sleeping and resending
	var f *protocol.Frame
	for attempt := 0; attempt <= maxRetries; attempt++ {
		for {
			resp, err := readFrameLine(conn, r, timeoutSec)
			if err != nil {
				fmt.Fprintf(os.Stderr, "heptaminic: read response: %v\n", err)
				os.Exit(1)
			}
			printFrame(resp, csv, quiet)

			if resp.Verb == protocol.VerbConfirm {
				exitOnConfirm(resp)
			}

			// Server can yield under load.
			if resp.Status == protocol.StatusYield {
				if attempt == maxRetries {
					fmt.Fprintln(os.Stderr, "heptaminic: max_retries reached on manifest yield")
					os.Exit(1)
				}
				sleepOnYield(resp, maxRetryMs)
				// resend manifest with next seq
				manifest.Seq = seq
				seq++
				if err := writeFrameLine(conn, w, &manifest, timeoutSec); err != nil {
					fmt.Fprintf(os.Stderr, "heptaminic: rewrite manifest: %v\n", err)
					os.Exit(1)
				}
				// After resending, restart inner read loop.
				continue
			}

			// Clarify is the acceptance boundary for the manifest in v0.
			if resp.Verb == protocol.VerbClarify {
				f = resp
				break
			}

			// Ignore other verbs for v0 (future extensions).
		}
		if f != nil {
			break
		}
	}

	if f == nil {
		fmt.Fprintln(os.Stderr, "heptaminic: no manifest response")
		os.Exit(1)
	}
	if f.Verb != protocol.VerbClarify {
		fmt.Fprintf(os.Stderr, "heptaminic: expected clarify response to manifest, got verb=%q\n", f.Verb)
		os.Exit(1)
	}
	if f.Status == protocol.StatusNeed {
		fmt.Fprintf(os.Stderr, "heptaminic: server needs correction: need_code=%q err=%q\n", f.NeedCode, f.ErrorCode)
		os.Exit(1)
	}

	// 2) share chunks in-order
	for i, b := range chunks {
		isLast := i == len(chunks)-1

		chSum := sha256.Sum256(b)
		chHex := hex.EncodeToString(chSum[:])

		ch := protocol.Frame{
			V:     0,
			Verb:  protocol.VerbShare,
			Job:   job,
			Seq:   seq,
			TTLms: 0,
			PayloadChunk: &protocol.PayloadChunk{
				PayloadID: payloadID,
				Index:     uint64(i),
				Offset:    uint64(i * chunkBytes),
				Sha256Hex: chHex,
				BytesB64:  base64.StdEncoding.EncodeToString(b),
			},
		}
		seq++

		if err := writeFrameLine(conn, w, &ch, timeoutSec); err != nil {
			fmt.Fprintf(os.Stderr, "heptaminic: write chunk %d: %v\n", i, err)
			os.Exit(1)
		}

		// Server may reply 1+ frames; we read until we see either:
		// - clarify (for chunk acceptance)
		// - confirm (final close)
		for attempt := 0; attempt <= maxRetries; attempt++ {
			resp, err := readFrameLine(conn, r, timeoutSec)
			if err != nil {
				fmt.Fprintf(os.Stderr, "heptaminic: read after chunk %d: %v\n", i, err)
				os.Exit(1)
			}
			printFrame(resp, csv, quiet)

			if resp.Verb == protocol.VerbConfirm {
				exitOnConfirm(resp)
			}

			if resp.Status == protocol.StatusYield {
				if maxRetries <= 0 {
					fmt.Fprintln(os.Stderr, "heptaminic: server yielded and retries disabled")
					os.Exit(1)
				}
				if attempt == maxRetries {
					fmt.Fprintf(os.Stderr, "heptaminic: max_retries reached on yield after chunk %d\n", i)
					os.Exit(1)
				}
				// Back off, then resend the same chunk with a new seq.
				sleepOnYield(resp, maxRetryMs)
				ch.Seq = seq
				seq++
				if err := writeFrameLine(conn, w, &ch, timeoutSec); err != nil {
					fmt.Fprintf(os.Stderr, "heptaminic: rewrite chunk %d: %v\n", i, err)
					os.Exit(1)
				}
				// Continue reading responses for the resent chunk.
				continue
			}

			if resp.Verb == protocol.VerbClarify {
				if resp.Status == protocol.StatusNeed {
					fmt.Fprintf(os.Stderr, "heptaminic: server needs correction after chunk %d: need_code=%q err=%q\n", i, resp.NeedCode, resp.ErrorCode)
					os.Exit(1)
				}
				// For non-final chunks, clarify(ok) is the acceptance boundary.
				// For the final chunk, the server may send clarify(ok) and then confirm(done).
				if !isLast {
					break
				}
				continue
			}

			// Ignore other verbs for v0 (future extensions).
		}
	}

	// If server never sent confirm (should in v0), exit nonzero.
	fmt.Fprintln(os.Stderr, "heptaminic: finished sending chunks but did not receive confirm")
	os.Exit(1)
}

func deriveJob(path string) string {
	base := filepath.Base(path)
	base = strings.TrimSpace(base)
	base = strings.ReplaceAll(base, " ", "_")
	if base == "" {
		return "J_1"
	}
	return "J_" + base
}

func splitChunks(b []byte, n int) [][]byte {
	if len(b) == 0 {
		// represent empty payload as one empty chunk
		return [][]byte{[]byte{}}
	}
	var out [][]byte
	for i := 0; i < len(b); i += n {
		j := i + n
		if j > len(b) {
			j = len(b)
		}
		out = append(out, b[i:j])
	}
	return out
}

func writeFrameLine(conn net.Conn, w *bufio.Writer, f *protocol.Frame, timeoutSec int) error {
	if conn != nil && timeoutSec > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(timeoutSec) * time.Second))
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

func readFrameLine(conn net.Conn, r *bufio.Reader, timeoutSec int) (*protocol.Frame, error) {
	if conn != nil && timeoutSec > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(timeoutSec) * time.Second))
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	var f protocol.Frame
	if err := json.Unmarshal([]byte(line), &f); err != nil {
		return nil, err
	}
	return &f, nil
}

func printFrame(f *protocol.Frame, csv bool, quiet bool) {
	ts := time.Now().UnixMilli()
	if csv {
		fmt.Printf("%d,%s,%s,%d,%s,%s,%d,%s\n", ts, f.Verb, f.Job, f.Seq, f.Status, f.NeedCode, f.RetryHintMs, sanitizeCSV(sanitize(f.ErrorCode)))
		return
	}
	if quiet {
		return
	}
	// Print raw minimal summary; keep stable for scripts.
	// (Don’t dump payload bytes.)
	fmt.Printf("recv v=%d verb=%s job=%s seq=%d status=%s need=%s err=%s\n",
		f.V, f.Verb, f.Job, f.Seq, f.Status, f.NeedCode, sanitize(f.ErrorCode))
}

func sanitize(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(s) > 160 {
		return s[:160] + "…"
	}
	return s
}

func sanitizeCSV(s string) string {
	// Keep CSV single-line and reasonably safe.
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, ",", ";")
	return s
}

func sleepOnYield(f *protocol.Frame, maxRetryMs int) {
	ms := int(f.RetryHintMs)
	if ms <= 0 {
		ms = 50
	}
	if maxRetryMs > 0 && ms > maxRetryMs {
		ms = maxRetryMs
	}
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func exitOnConfirm(f *protocol.Frame) {
	switch f.Status {
	case protocol.StatusDone:
		os.Exit(0)
	case protocol.StatusAbort:
		os.Exit(2)
	default:
		// confirm without done/abort is weird in v0
		os.Exit(1)
	}
}
