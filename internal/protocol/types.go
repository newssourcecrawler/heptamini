package protocol

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

type Verb string

const (
	// Upload request / data frames.
	VerbShare Verb = "share"

	// Server replies.
	VerbClarify Verb = "clarify"
	VerbConfirm Verb = "confirm"

	// Health/liveness probe.
	VerbPing Verb = "ping"
)

type Status string

const (
	StatusOK    Status = "ok"
	StatusNeed  Status = "need"
	StatusYield Status = "yield"
	StatusDone  Status = "done"
	StatusAbort Status = "abort"
)

type Frame struct {
	V      int    `json:"v"`
	Verb   Verb   `json:"verb"`
	Job    string `json:"job"`
	Seq    uint64 `json:"seq"`
	TTLms  uint64 `json:"ttl_ms"`
	Status Status `json:"status,omitempty"`

	// Upload protocol (v1)
	PayloadManifest *PayloadManifest `json:"payload_manifest,omitempty"`
	PayloadChunk    *PayloadChunk    `json:"payload_chunk,omitempty"`

	// Server-side hints (requests MUST NOT set these).
	NeedCode    string `json:"need_code,omitempty"`
	RetryHintMs uint64 `json:"retry_hint_ms,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`

	Receipts []Receipt `json:"receipts,omitempty"`
}

type Receipt struct {
	Kind     string `json:"kind"`
	ValueU64 uint64 `json:"value_u64"`
}

type PayloadManifest struct {
	PayloadID        string `json:"payload_id"`
	ContentType      string `json:"content_type"`
	ContentEncoding  string `json:"content_encoding"` // v1: "identity" only (declared)
	Cipher           string `json:"cipher"`           // v1: "none" (declared)
	TotalBytes       uint64 `json:"total_bytes"`
	ChunkBytes       uint64 `json:"chunk_bytes"`
	Chunks           uint64 `json:"chunks"`
	Sha256HexEncoded string `json:"sha256"` // sha256 over encoded stream
}

type PayloadChunk struct {
	PayloadID string `json:"payload_id"`
	Index     uint64 `json:"index"`
	Offset    uint64 `json:"offset"`
	Sha256Hex string `json:"sha256"`
	BytesB64  string `json:"bytes_b64"`
}

func ValidateEnvelope(f *Frame) error {
	if f == nil {
		return errors.New("nil frame")
	}
	if f.V != 0 {
		return errors.New("unsupported v")
	}
	if f.Job == "" {
		return errors.New("missing job")
	}
	if f.Seq == 0 {
		return errors.New("seq must be >= 1")
	}
	if f.Verb == "" {
		return errors.New("missing verb")
	}

	// v1: only these verbs exist.
	switch f.Verb {
	case VerbShare, VerbClarify, VerbConfirm, VerbPing:
	default:
		return fmt.Errorf("unsupported verb: %q", f.Verb)
	}

	hasReceipts := len(f.Receipts) > 0

	// v1: disallow ambiguous payload combinations.
	hasManifest := f.PayloadManifest != nil
	hasChunk := f.PayloadChunk != nil
	if hasManifest && hasChunk {
		return errors.New("both payload_manifest and payload_chunk set")
	}

	// v1 verb-specific shape rules.
	switch f.Verb {
	case VerbPing:
		if hasManifest || hasChunk {
			return errors.New("ping must not include payload")
		}
		if f.Status != "" {
			return errors.New("ping request must not set status")
		}
		if f.NeedCode != "" || f.ErrorCode != "" || f.RetryHintMs != 0 || hasReceipts {
			return errors.New("ping request must not set server fields")
		}
	case VerbShare:
		// Client upload frames.
		if !(hasManifest || hasChunk) {
			return errors.New("share requires payload_manifest or payload_chunk")
		}
		if f.Status != "" {
			return errors.New("share request must not set status")
		}
		if f.NeedCode != "" || f.ErrorCode != "" || f.RetryHintMs != 0 || hasReceipts {
			return errors.New("share request must not set server fields")
		}
	case VerbClarify, VerbConfirm:
		// Server replies.
		if f.Status == "" {
			return errors.New("server reply missing status")
		}
		if hasManifest || hasChunk {
			return errors.New("server reply must not include payload")
		}
	}

	return nil
}

func DecodeChunkBytes(ch *PayloadChunk) ([]byte, error) {
	if ch == nil {
		return nil, errors.New("nil chunk")
	}
	b, err := base64.StdEncoding.DecodeString(ch.BytesB64)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func VerifySHA256Hex(data []byte, wantHex string) bool {
	wantHex = strings.ToLower(strings.TrimSpace(wantHex))
	if wantHex == "" {
		return false
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	return got == wantHex
}
