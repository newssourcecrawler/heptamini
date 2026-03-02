package protocol

import (
	"errors"
	"fmt"
)

const (
	// v1 safety cap: server must not buffer unbounded payloads.
	// Keep this boring and constant for v1; expose later if needed.
	maxUploadBytes = 50 * 1024 * 1024
)

type UploadState struct {
	Job string

	Manifest *PayloadManifest

	NextIndex uint64
	BytesSeen uint64

	EncodedSHA256Hex string

	// v1: in-order upload; bytes are buffered until terminal verification.
	Buf []byte
}

func NewUploadState(job string) *UploadState {
	return &UploadState{Job: job}
}

func (u *UploadState) ApplyManifest(m *PayloadManifest) error {
	if m == nil {
		return errors.New("nil manifest")
	}
	if m.PayloadID == "" {
		return errors.New("manifest missing payload_id")
	}
	if m.ChunkBytes == 0 {
		return errors.New("manifest chunk_bytes=0")
	}
	if m.Chunks == 0 {
		return errors.New("manifest chunks=0")
	}

	// v1 posture: declared-only fields are fixed.
	if m.ContentEncoding != "" && m.ContentEncoding != "identity" {
		return fmt.Errorf("manifest content_encoding unsupported: %q", m.ContentEncoding)
	}
	if m.Cipher != "" && m.Cipher != "none" {
		return fmt.Errorf("manifest cipher unsupported: %q", m.Cipher)
	}

	// v1 safety: cap total buffered bytes. Prefer declared TotalBytes when present.
	expectedMax := m.ChunkBytes * m.Chunks
	if m.TotalBytes > 0 && m.TotalBytes < expectedMax {
		expectedMax = m.TotalBytes
	}
	if expectedMax > maxUploadBytes {
		return fmt.Errorf("manifest payload too large: %d bytes (cap %d)", expectedMax, maxUploadBytes)
	}

	// TotalBytes can be 0 for empty payloads; allow.
	u.Manifest = m
	u.EncodedSHA256Hex = m.Sha256HexEncoded
	u.NextIndex = 0
	u.BytesSeen = 0
	u.Buf = u.Buf[:0]
	return nil
}

type Need struct {
	Code string
	Msg  string
	Want uint64 // expected index
}

func (u *UploadState) ApplyChunk(ch *PayloadChunk) (*Need, error) {
	if u.Manifest == nil {
		return &Need{Code: "manifest_missing", Msg: "send payload_manifest first"}, nil
	}
	if ch == nil {
		return &Need{Code: "chunk_missing", Msg: "missing payload_chunk"}, nil
	}
	if ch.PayloadID != u.Manifest.PayloadID {
		return &Need{Code: "payload_id_mismatch", Msg: "chunk payload_id mismatch"}, nil
	}
	if ch.Index != u.NextIndex {
		return &Need{Code: "chunk_expected_index", Msg: fmt.Sprintf("expected index=%d", u.NextIndex), Want: u.NextIndex}, nil
	}

	b, err := DecodeChunkBytes(ch)
	if err != nil {
		return &Need{Code: "chunk_b64_invalid", Msg: "chunk bytes_b64 decode failed"}, nil
	}

	if uint64(len(b)) > u.Manifest.ChunkBytes {
		return &Need{Code: "chunk_too_large", Msg: fmt.Sprintf("chunk too large index=%d", ch.Index)}, nil
	}

	// Cumulative cap: prevent buffering beyond declared/derived maximum.
	maxBytes := u.Manifest.ChunkBytes * u.Manifest.Chunks
	if u.Manifest.TotalBytes > 0 && u.Manifest.TotalBytes < maxBytes {
		maxBytes = u.Manifest.TotalBytes
	}
	if u.BytesSeen+uint64(len(b)) > maxBytes {
		return &Need{Code: "payload_too_large", Msg: "payload exceeds declared size"}, nil
	}

	if !VerifySHA256Hex(b, ch.Sha256Hex) {
		return &Need{Code: "chunk_hash_mismatch", Msg: fmt.Sprintf("hash mismatch index=%d", ch.Index)}, nil
	}

	// In-order append.
	u.Buf = append(u.Buf, b...)
	u.BytesSeen += uint64(len(b))
	u.NextIndex++

	// If this was the last chunk, validate total stream hash if provided.
	if u.NextIndex == u.Manifest.Chunks {
		if u.Manifest.TotalBytes > 0 && u.BytesSeen != u.Manifest.TotalBytes {
			return &Need{Code: "payload_size_mismatch", Msg: fmt.Sprintf("payload bytes=%d want=%d", u.BytesSeen, u.Manifest.TotalBytes)}, nil
		}
		// total bytes is advisory; don't hard-fail if off (wild clients exist), but record later.
		if u.EncodedSHA256Hex != "" && !VerifySHA256Hex(u.Buf, u.EncodedSHA256Hex) {
			return &Need{Code: "payload_hash_mismatch", Msg: "payload sha256 mismatch (encoded stream)"}, nil
		}
	}

	return nil, nil
}

func (u *UploadState) IsComplete() bool {
	return u.Manifest != nil && u.NextIndex == u.Manifest.Chunks
}
